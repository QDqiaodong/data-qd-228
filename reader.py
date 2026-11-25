"""Bounded client-side conditional GET and in-process scheduling."""
from collections import OrderedDict, deque
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPException
import math
from queue import Queue
import re
import threading
import time
from urllib.parse import urlsplit


__all__ = [
    "ConditionalReader",
    "OriginError",
    "QueueFull",
    "Representation",
    "RequestCancelled",
    "ScheduledReader",
    "Ticket",
]


class OriginError(RuntimeError):
    pass


class QueueFull(RuntimeError):
    """Raised when submission would exceed waiter or pending-work capacity."""


class RequestCancelled(Exception):
    """Raised by a ticket when that ticket is cancelled."""


@dataclass(frozen=True)
class Representation:
    body: bytes
    revalidated: bool


@dataclass(frozen=True)
class _Entry:
    tag: str
    body: bytes


class ConditionalReader:
    def __init__(self, host, port, *, entries=16, max_body=65536, timeout=2):
        if any(type(n) is not int or n < 1 for n in (entries, max_body)):
            raise ValueError("entries and max_body must be positive integers")
        self._host, self._port = host, port
        self._limit, self._max_body, self._timeout = entries, max_body, timeout
        self._entries = OrderedDict()

    def get(self, target):
        if (not isinstance(target, str) or not target.startswith("/")
                or target.startswith("//") or "#" in target
                or any(not 0x21 <= ord(char) <= 0x7E for char in target)):
            raise ValueError("target must be an ASCII origin-form path/query")
        parts = urlsplit(target)
        if parts.scheme or parts.netloc:
            raise ValueError("absolute targets are not allowed")
        previous = self._entries.get(target)
        headers = {"If-None-Match": previous.tag} if previous else {}
        connection = HTTPConnection(self._host, self._port, timeout=self._timeout)
        try:
            connection.request("GET", target, headers=headers)
            with connection.getresponse() as response:
                tag = response.getheader("ETag")
                # Only retain single-representation, public data of the managed origin.
                retain = all(response.getheader(h) is None for h in (
                    "Vary", "Set-Cookie", "Cache-Control"))
                if response.status == 304:
                    if previous is None or tag != previous.tag:
                        self._entries.pop(target, None)
                        raise OriginError("304 did not confirm the requested validator")
                    if retain:
                        self._entries.move_to_end(target)
                    else:
                        self._entries.pop(target, None)
                    return Representation(previous.body, True)
                if response.status != 200:
                    if response.status in (404, 410):
                        self._entries.pop(target, None)
                    raise OriginError(f"origin returned HTTP {response.status}")
                length = response.getheader("Content-Length", "")
                if (response.getheader("Transfer-Encoding") is not None
                        or not re.fullmatch(r"[0-9]{1,20}", length)
                        or int(length) > self._max_body):
                    raise OriginError("200 has no supported bounded Content-Length")
                body = response.read(int(length))
                if len(body) != int(length):
                    raise OriginError("origin body ended before Content-Length")
        except (OSError, HTTPException) as exc:
            raise OriginError("HTTP transport failed") from exc
        finally:
            connection.close()
        self._entries.pop(target, None)
        if retain and tag and re.fullmatch(r'"[!#-~]*"', tag):
            self._entries[target] = _Entry(tag, body)
            if len(self._entries) > self._limit:
                self._entries.popitem(last=False)
        return Representation(body, False)


_STRONG_TAG_RE = re.compile(r'"[!#-~]*"')
_LENGTH_RE = re.compile(r"[0-9]{1,20}")

_ACTIVE = "active"
_OK = "ok"
_TIMEOUT = "timeout"
_CANCELLED = "cancelled"
_ERROR = "error"

_QUEUED = "queued"
_RUNNING = "running"
_DONE = "done"

# With a caller-supplied (possibly manually advanced) clock, real time cannot be
# mapped to that clock.  Periodically rechecking gives operations a chance to
# observe an advanced clock without a thread per ticket.
_CUSTOM_CLOCK_POLL = 0.02
_REAL_CLOCK_WAIT_CAP = 3600.0


def _validate_target(target):
    if (not isinstance(target, str) or not target.startswith("/")
            or target.startswith("//") or "#" in target
            or any(not 0x21 <= ord(char) <= 0x7E for char in target)):
        raise ValueError("target must be an ASCII origin-form path/query")
    parts = urlsplit(target)
    if parts.scheme or parts.netloc:
        raise ValueError("absolute targets are not allowed")


def _valid_deadline(deadline):
    return (deadline is None
            or (not isinstance(deadline, bool)
                and isinstance(deadline, (int, float))
                and math.isfinite(deadline)))


def _strict_bounded_int(value, name, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(
            f"{name} must be an integer in [{lower}, {upper}]")


class Ticket:
    """Independent terminal state for one scheduled consumer."""

    __slots__ = ("_reader", "_deadline", "_state", "_value", "_error", "_work")

    def __init__(self, reader, deadline):
        self._reader = reader
        self._deadline = deadline
        self._state = _ACTIVE
        self._value = None
        self._error = None
        self._work = None

    def result(self):
        return self._reader._ticket_result(self)

    def cancel(self):
        return self._reader._ticket_cancel(self)


class _Work:
    __slots__ = ("seq", "target", "tickets", "active", "state",
                 "sent_tag", "sent_body")

    def __init__(self, seq, target):
        self.seq = seq
        self.target = target
        self.tickets = set()
        self.active = 0
        self.state = _QUEUED
        self.sent_tag = None
        self.sent_body = None


class _Outcome:
    __slots__ = ("representation", "error", "action",
                 "retain", "tag", "body")

    def __init__(self, *, representation=None, error=None, action="error",
                 retain=False, tag=None, body=None):
        self.representation = representation
        self.error = error
        self.action = action
        self.retain = retain
        self.tag = tag
        self.body = body


class ScheduledReader:
    """Bounded concurrent scheduler around one managed HTTP origin."""

    def __init__(self, host, port, *, parallelism=2, max_pending=16,
                 max_waiters=64, entries=16, max_body=65536, timeout=2,
                 clock=time.monotonic):
        _strict_bounded_int(parallelism, "parallelism", 1, 8)
        _strict_bounded_int(max_pending, "max_pending", 0, 64)
        _strict_bounded_int(max_waiters, "max_waiters", 1, 128)
        if type(entries) is not int or entries < 1:
            raise ValueError("entries must be a positive integer")
        if type(max_body) is not int or max_body < 1:
            raise ValueError("max_body must be a positive integer")

        self._host = host
        self._port = port
        self._parallelism = parallelism
        self._max_pending = max_pending
        self._max_waiters = max_waiters
        self._limit = entries
        self._max_body = max_body
        self._timeout = timeout
        self._clock = clock
        self._clock_is_real = clock is time.monotonic

        self._cache = OrderedDict()
        self._cond = threading.Condition(threading.RLock())
        self._pending = deque()
        self._joinable = {}
        self._busy_targets = set()
        self._tickets = set()
        self._waiters = 0
        self._active_count = 0
        self._next_seq = 0
        self._closed = False

        self._stop = object()
        self._run_queue = Queue()
        self._workers = []
        for index in range(parallelism):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"ScheduledReader-worker-{id(self)}-{index}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def submit(self, target, *, deadline=None):
        _validate_target(target)
        if not _valid_deadline(deadline):
            raise ValueError(
                "deadline must be None or a finite int/float (not bool)")

        ticket = Ticket(self, deadline)
        with self._cond:
            try:
                if self._closed:
                    raise RuntimeError("ScheduledReader is closed")

                now = self._clock()
                self._expire_locked(now)

                # An already-expired legal submission is terminal before any
                # capacity is consumed.
                if deadline is not None and now >= deadline:
                    ticket._state = _TIMEOUT
                    ticket._error = TimeoutError("ticket deadline exceeded")
                    return ticket

                # Give already-queued eligible work any free slots before
                # deciding whether a new item needs pending capacity.
                self._pump_locked()

                if self._waiters >= self._max_waiters:
                    raise QueueFull("too many unterminated tickets")

                work = self._joinable.get(target)
                if work is not None and work.active <= 0:
                    self._joinable.pop(target, None)
                    work = None

                if work is None:
                    dispatchable_now = (
                        target not in self._busy_targets
                        and self._active_count < self._parallelism
                    )
                    if (not dispatchable_now
                            and len(self._pending) >= self._max_pending):
                        raise QueueFull("too many pending work items")

                    work = _Work(self._next_seq, target)
                    self._next_seq += 1
                    self._pending.append(work)
                    self._joinable[target] = work

                ticket._work = work
                work.tickets.add(ticket)
                work.active += 1
                self._tickets.add(ticket)
                self._waiters += 1

                self._pump_locked()
                return ticket
            finally:
                self._cond.notify_all()

    def get(self, target):
        """Convenience blocking read backed by an unlimited scheduled ticket."""
        return self.submit(target).result()

    def close(self):
        with self._cond:
            if not self._closed:
                self._closed = True
                self._expire_locked(self._clock())
                for ticket in list(self._tickets):
                    self._settle_locked(
                        ticket, _CANCELLED,
                        error=RequestCancelled("ScheduledReader closed"))
                self._pending.clear()
                self._joinable.clear()
                for _ in self._workers:
                    self._run_queue.put(self._stop)
                self._cond.notify_all()

        for worker in self._workers:
            worker.join()

    # ------------------------------------------------------------------
    # Ticket state transitions
    # ------------------------------------------------------------------

    def _ticket_result(self, ticket):
        with self._cond:
            while True:
                now = self._clock()
                if self._expire_locked(now):
                    self._pump_locked()
                    self._cond.notify_all()

                if ticket._state == _OK:
                    return ticket._value
                if ticket._state != _ACTIVE:
                    raise ticket._error

                deadline = ticket._deadline
                if deadline is not None and now >= deadline:
                    # _expire_locked has already terminalized this ticket.
                    continue

                if deadline is None:
                    self._cond.wait()
                elif self._clock_is_real:
                    remaining = deadline - now
                    if remaining > _REAL_CLOCK_WAIT_CAP:
                        self._cond.wait(_REAL_CLOCK_WAIT_CAP)
                    else:
                        self._cond.wait(max(0.0, remaining))
                else:
                    # Fake clocks are advanced by tests and followed by an
                    # operation/notification; this short recheck also catches
                    # an advance if no other operation occurs.
                    self._cond.wait(_CUSTOM_CLOCK_POLL)

    def _ticket_cancel(self, ticket):
        with self._cond:
            now = self._clock()
            if self._expire_locked(now):
                self._pump_locked()
                self._cond.notify_all()

            if ticket._state != _ACTIVE:
                return False

            changed = self._settle_locked(
                ticket, _CANCELLED,
                error=RequestCancelled("request cancelled by consumer"))
            if changed:
                self._pump_locked()
                self._cond.notify_all()
            return changed

    def _settle_locked(self, ticket, state, *, value=None, error=None):
        if ticket._state != _ACTIVE:
            return False

        ticket._state = state
        ticket._value = value
        ticket._error = error
        self._waiters -= 1
        self._tickets.discard(ticket)

        work = ticket._work
        if work is not None:
            work.tickets.discard(ticket)
            work.active -= 1
            if work.active == 0:
                self._work_became_empty_locked(work)
        return True

    def _expire_locked(self, now):
        changed = False
        for ticket in list(self._tickets):
            deadline = ticket._deadline
            if deadline is not None and now >= deadline:
                self._settle_locked(
                    ticket, _TIMEOUT,
                    error=TimeoutError("ticket deadline exceeded"))
                changed = True
        return changed

    def _work_became_empty_locked(self, work):
        if self._joinable.get(work.target) is work:
            self._joinable.pop(work.target, None)
        if work.state == _QUEUED:
            try:
                self._pending.remove(work)
            except ValueError:
                pass

    # ------------------------------------------------------------------
    # Dispatch and workers
    # ------------------------------------------------------------------

    def _pump_locked(self):
        while not self._closed and self._active_count < self._parallelism:
            selected = None
            for work in self._pending:
                # A target with a dispatched request stays blocked until that
                # connection is closed and its completion is registered.
                if work.target not in self._busy_targets:
                    selected = work
                    break
            if selected is None:
                return

            self._pending.remove(selected)
            selected.state = _RUNNING
            self._busy_targets.add(selected.target)
            self._active_count += 1
            self._run_queue.put(selected)

    def _worker_loop(self):
        while True:
            item = self._run_queue.get()
            if item is self._stop:
                return

            work = item
            outcome = None
            should_fetch = False
            try:
                with self._cond:
                    self._expire_locked(self._clock())
                    should_fetch = work.active > 0
                    if should_fetch:
                        previous = self._cache.get(work.target)
                        if previous is None:
                            work.sent_tag = None
                            work.sent_body = None
                        else:
                            work.sent_tag = previous.tag
                            work.sent_body = previous.body

                if should_fetch:
                    outcome = self._fetch(work)
            except Exception as exc:  # never leak a scheduler slot
                outcome = _Outcome(
                    error=OriginError("HTTP transport failed"),
                    action="error")
                outcome.error.__cause__ = exc

            try:
                with self._cond:
                    self._finish_locked(work, outcome)
            except Exception:
                # The locked finish is intentionally simple; this guard keeps
                # a worker thread alive if an unexpected defect occurs.
                pass

    def _finish_locked(self, work, outcome):
        try:
            # Completion time, not send/header time, decides ticket validity.
            self._expire_locked(self._clock())

            if work.active > 0:
                if outcome is None:
                    outcome = _Outcome(
                        error=OriginError("scheduled work did not complete"),
                        action="error")
                self._commit_cache_locked(work, outcome)
                tickets = list(work.tickets)
                for ticket in tickets:
                    if outcome.error is None:
                        self._settle_locked(
                            ticket, _OK, value=outcome.representation)
                    else:
                        error = type(outcome.error)(str(outcome.error))
                        error.__cause__ = outcome.error.__cause__
                        self._settle_locked(ticket, _ERROR, error=error)
            # If there is no valid ticket, deliberately do not touch cache or
            # deliver to any newer replacement work.
        finally:
            work.state = _DONE
            self._busy_targets.discard(work.target)
            self._active_count -= 1
            if self._joinable.get(work.target) is work:
                self._joinable.pop(work.target, None)
            self._pump_locked()
            self._cond.notify_all()

    def _commit_cache_locked(self, work, outcome):
        cache = self._cache
        target = work.target

        if outcome.action == "ok200":
            cache.pop(target, None)
            if (outcome.retain and outcome.tag
                    and _STRONG_TAG_RE.fullmatch(outcome.tag)):
                cache[target] = _Entry(outcome.tag, outcome.body)
                if len(cache) > self._limit:
                    cache.popitem(last=False)

        elif outcome.action == "not_modified":
            if outcome.retain and work.sent_tag is not None:
                # Reinsert the exact confirmed entity.  It may have been
                # evicted by another target while this request was in flight.
                cache.pop(target, None)
                cache[target] = _Entry(work.sent_tag, work.sent_body)
                if len(cache) > self._limit:
                    cache.popitem(last=False)
            else:
                cache.pop(target, None)

        elif outcome.action == "clear":
            cache.pop(target, None)

        # action == "error": intentionally no content/order change.

    # ------------------------------------------------------------------
    # HTTP; runs outside the scheduler lock
    # ------------------------------------------------------------------

    def _fetch(self, work):
        target = work.target
        headers = ({"If-None-Match": work.sent_tag}
                   if work.sent_tag is not None else {})
        connection = HTTPConnection(
            self._host, self._port, timeout=self._timeout)
        outcome = None

        try:
            try:
                connection.request("GET", target, headers=headers)
                with connection.getresponse() as response:
                    response_tag = response.getheader("ETag")
                    retain = all(response.getheader(h) is None for h in (
                        "Vary", "Set-Cookie", "Cache-Control"))

                    if response.status == 304:
                        if (work.sent_tag is None
                                or response_tag != work.sent_tag):
                            error = OriginError(
                                "304 did not confirm the requested validator")
                            outcome = _Outcome(error=error, action="clear")
                        else:
                            representation = Representation(
                                work.sent_body, True)
                            outcome = _Outcome(
                                representation=representation,
                                action="not_modified",
                                retain=retain)

                    elif response.status != 200:
                        error = OriginError(
                            f"origin returned HTTP {response.status}")
                        action = ("clear"
                                  if response.status in (404, 410)
                                  else "error")
                        outcome = _Outcome(error=error, action=action)

                    else:
                        length = response.getheader("Content-Length", "")
                        if (response.getheader("Transfer-Encoding") is not None
                                or not _LENGTH_RE.fullmatch(length)
                                or int(length) > self._max_body):
                            raise OriginError(
                                "200 has no supported bounded Content-Length")

                        body = response.read(int(length))
                        if len(body) != int(length):
                            raise OriginError(
                                "origin body ended before Content-Length")

                        outcome = _Outcome(
                            representation=Representation(body, False),
                            action="ok200",
                            retain=retain,
                            tag=response_tag,
                            body=body)

            except OriginError as exc:
                if outcome is None:
                    outcome = _Outcome(error=exc, action="error")
            except (OSError, HTTPException) as exc:
                error = OriginError("HTTP transport failed")
                error.__cause__ = exc
                outcome = _Outcome(error=error, action="error")

            return outcome
        finally:
            connection.close()
