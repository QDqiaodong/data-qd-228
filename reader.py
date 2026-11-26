"""Bounded client-side conditional GET and whole-release reading."""
from collections import OrderedDict, deque
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPException
import hashlib
import json
import math
import os
from queue import Queue
import re
import shutil
import tempfile
import threading
import time
from urllib.parse import urlsplit


__all__ = [
    "ConditionalReader",
    "OriginError",
    "QueueFull",
    "ReleaseReader",
    "ReleaseUnavailable",
    "Representation",
    "RequestCancelled",
    "ScheduledReader",
    "Ticket",
]

# Binary units and release-file bounds from the publishing contract.
_KIB = 1024
_MIB = 1024 * 1024
_MANIFEST_LIMIT = 32 * _KIB
_MEMBER_LIMIT = _MIB
_TARGET_LIMIT = 256
_MAX_MEMBERS = 64
_VERSION_MAX = (1 << 63) - 1
_IO_CHUNK = 64 * _KIB

_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class OriginError(RuntimeError):
    pass


class ReleaseUnavailable(OriginError):
    """No verified whole release is currently available for local reads."""


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


def _validate_release_target(target):
    """Validate the exact ASCII origin target and release length bound."""
    _validate_target(target)
    if len(target.encode("ascii")) > _TARGET_LIMIT:
        raise ValueError(
            f"target must be at most {_TARGET_LIMIT} UTF-8/ASCII bytes")


def _reject_duplicate_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise OriginError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _strict_manifest_int(value, field, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise OriginError(
            f"{field} must be an integer in [{lower}, {upper}] (not bool)")
    return value


def _parse_release_manifest(raw, manifest_target):
    try:
        text = raw.decode("utf-8", errors="strict")
        manifest_object = json.loads(
            text, object_pairs_hook=_reject_duplicate_json_keys)
    except OriginError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise OriginError("release manifest is not valid UTF-8 JSON") from exc

    if type(manifest_object) is not dict:
        raise OriginError("release manifest must be a JSON object")
    if set(manifest_object) != {"version", "serve_min", "members"}:
        raise OriginError(
            "release manifest fields must be exactly version, serve_min, members")

    version = _strict_manifest_int(
        manifest_object["version"], "version", 1, _VERSION_MAX)
    serve_min = _strict_manifest_int(
        manifest_object["serve_min"], "serve_min", 0, version)

    raw_members = manifest_object["members"]
    if type(raw_members) is not list or not 1 <= len(raw_members) <= _MAX_MEMBERS:
        raise OriginError("members must be a list of 1..64 records")

    members = {}
    for index, record in enumerate(raw_members):
        if type(record) is not dict:
            raise OriginError(f"member {index} must be an object")
        if set(record) != {"target", "size", "sha256"}:
            raise OriginError(
                f"member {index} fields must be exactly target, size, sha256")

        target = record["target"]
        try:
            _validate_release_target(target)
        except ValueError as exc:
            raise OriginError(f"member {index} has an invalid target") from exc
        if target == manifest_target:
            raise OriginError("a member target cannot be the manifest target")
        if target in members:
            raise OriginError(f"duplicate member target: {target}")

        size = _strict_manifest_int(
            record["size"], f"member {index} size", 0, _MEMBER_LIMIT)
        digest = record["sha256"]
        if type(digest) is not str or _SHA256_RE.fullmatch(digest) is None:
            raise OriginError(
                f"member {index} sha256 must be 64 lowercase hex digits")

        members[target] = _MemberSpec(target, size, digest)

    return _Manifest(version, serve_min, members)


def _accepted_get_length(response, max_length):
    """Check the strict response framing/retention policy and return length."""
    if response.headers.get_all("Transfer-Encoding"):
        raise OriginError("responses with Transfer-Encoding are not accepted")

    encodings = response.headers.get_all("Content-Encoding") or ()
    for encoding in encodings:
        codings = [part.strip().lower() for part in encoding.split(",")]
        if any(coding != "identity" for coding in codings):
            raise OriginError("only identity Content-Encoding is accepted")

    for header in ("Vary", "Set-Cookie", "Cache-Control"):
        if response.headers.get_all(header):
            raise OriginError(f"responses with {header} are not installable")

    lengths = response.headers.get_all("Content-Length")
    if (len(lengths or ()) != 1
            or _LENGTH_RE.fullmatch(lengths[0]) is None):
        raise OriginError("200 must have exactly one bounded Content-Length")
    length = int(lengths[0])
    if length > max_length:
        raise OriginError("200 Content-Length exceeds the declared bound")
    return length


@dataclass(frozen=True)
class _MemberSpec:
    target: str
    size: int
    sha256: str


@dataclass(frozen=True)
class _Manifest:
    version: int
    serve_min: int
    members: dict


@dataclass(frozen=True)
class _StoredMember:
    target: str
    size: int
    sha256: str
    path: str


class _ActiveRelease:
    __slots__ = ("version", "directory", "members")

    def __init__(self, version, directory, members):
        self.version = version
        self.directory = directory
        self.members = members


class ReleaseReader:
    """Read and atomically install whole configuration releases from one origin."""

    def __init__(self, host, port, manifest_target, *, scratch_dir, timeout=2):
        # Validate caller-visible target arguments before creating workspace.
        _validate_release_target(manifest_target)
        self._host = host
        self._port = port
        self._manifest_target = manifest_target
        self._timeout = timeout

        # mkdtemp gives this instance a private subtree; close() removes only it.
        scratch_path = os.path.abspath(os.fspath(scratch_dir))
        self._root = tempfile.mkdtemp(
            prefix="release-reader-", dir=scratch_path)
        self._closed = False
        self._baseline = None
        self._active = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    @property
    def active_version(self):
        if self._closed or self._active is None:
            return None
        return self._active.version

    def refresh(self):
        if self._closed:
            raise RuntimeError("ReleaseReader is closed")

        manifest = self._fetch_and_parse_manifest()
        self._advance_baseline(manifest)

        active_at_start = self._active
        if self._manifest_is_active(manifest):
            return manifest.version

        stage_dir = os.path.join(self._root, "stage")
        must_retire = (
            active_at_start is not None
            and manifest.serve_min > active_at_start.version)
        try:
            self._remove_transient_paths()
            os.mkdir(stage_dir)

            if must_retire:
                # Publishing the retirement decision in memory first makes
                # the old release unreadable even if candidate population or
                # a later member download fails.  The old directory stays
                # available only long enough to hardlink/copy declared reuse.
                self._active = None
                try:
                    reused = self._populate_reused(
                        manifest, active_at_start, stage_dir)
                finally:
                    shutil.rmtree(
                        active_at_start.directory, ignore_errors=True)
            else:
                reused = self._populate_reused(
                    manifest, active_at_start, stage_dir)

            self._download_missing_members(manifest, reused, stage_dir)
            return self._commit_release(manifest, stage_dir)
        except OriginError:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise
        except (OSError, HTTPException) as exc:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise OriginError("failed to install complete release") from exc

    def read_active(self, target):
        if self._closed:
            raise RuntimeError("ReleaseReader is closed")

        active = self._active
        if active is None:
            raise ReleaseUnavailable("no configuration release is active")
        _validate_release_target(target)

        member = active.members.get(target)
        if member is None:
            raise KeyError(target)

        try:
            # Member size is at most 1 MiB; this is the only whole-body
            # allocation exposed by read_active().
            with open(member.path, "rb") as handle:
                return handle.read()
        except OSError as exc:
            raise ReleaseUnavailable("active release bytes are unavailable") from exc

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._active = None
        self._baseline = None
        shutil.rmtree(self._root, ignore_errors=True)

    # ------------------------------------------------------------------
    # Manifest and baseline
    # ------------------------------------------------------------------

    def _fetch_and_parse_manifest(self):
        raw = self._http_get_manifest()
        return _parse_release_manifest(raw, self._manifest_target)

    def _http_get_manifest(self):
        connection = HTTPConnection(
            self._host, self._port, timeout=self._timeout)
        try:
            try:
                connection.request(
                    "GET", self._manifest_target,
                    headers={"Connection": "close"})
                with connection.getresponse() as response:
                    if response.status != 200:
                        raise OriginError(
                            f"manifest origin returned HTTP {response.status}")
                    length = _accepted_get_length(response, _MANIFEST_LIMIT)
                    raw = response.read(length)
                    if len(raw) != length:
                        raise OriginError("manifest response ended early")
            except OriginError:
                raise
            except (OSError, HTTPException) as exc:
                raise OriginError("manifest GET failed") from exc
        finally:
            connection.close()
        return raw

    def _advance_baseline(self, manifest):
        baseline = self._baseline
        if baseline is not None:
            if manifest.version < baseline.version:
                raise OriginError("release version would move backwards")
            if manifest.version == baseline.version:
                if (manifest.serve_min != baseline.serve_min
                        or manifest.members != baseline.members):
                    raise OriginError(
                        "same release version cannot describe another manifest")
            elif manifest.serve_min < baseline.serve_min:
                raise OriginError("minimum service version would move backwards")
        self._baseline = manifest

    def _manifest_is_active(self, manifest):
        active = self._active
        if active is None or active.version != manifest.version:
            return False
        if len(active.members) != len(manifest.members):
            return False
        for spec in manifest.members.values():
            stored = active.members.get(spec.target)
            if (stored is None or stored.size != spec.size
                    or stored.sha256 != spec.sha256):
                return False
        return True

    # ------------------------------------------------------------------
    # Workspace, reuse, downloads, and commit
    # ------------------------------------------------------------------

    def _remove_transient_paths(self):
        for name in ("stage", "old"):
            shutil.rmtree(os.path.join(self._root, name), ignore_errors=True)

    @staticmethod
    def _member_filename(index):
        return f"member-{index:03d}"

    def _populate_reused(self, manifest, active, stage_dir):
        reused = set()
        if active is None:
            return reused

        for index, spec in enumerate(manifest.members.values()):
            previous = active.members.get(spec.target)
            if (previous is None or previous.size != spec.size
                    or previous.sha256 != spec.sha256):
                continue

            destination = os.path.join(stage_dir, self._member_filename(index))
            self._link_or_copy_verified(previous.path, destination)
            reused.add(spec.target)
        return reused

    @staticmethod
    def _link_or_copy_verified(source, destination):
        try:
            os.link(source, destination)
        except OSError:
            with open(source, "rb") as src, open(destination, "wb") as dst:
                shutil.copyfileobj(src, dst, length=_IO_CHUNK)

    def _download_missing_members(self, manifest, reused, stage_dir):
        for index, spec in enumerate(manifest.members.values()):
            if spec.target in reused:
                continue
            destination = os.path.join(stage_dir, self._member_filename(index))
            self._download_member(spec, destination)

    def _download_member(self, spec, destination):
        connection = HTTPConnection(
            self._host, self._port, timeout=self._timeout)
        try:
            try:
                connection.request(
                    "GET", spec.target, headers={"Connection": "close"})
                with connection.getresponse() as response:
                    if response.status != 200:
                        raise OriginError(
                            f"member origin returned HTTP {response.status} "
                            f"for {spec.target}")
                    length = _accepted_get_length(response, spec.size)
                    if length != spec.size:
                        raise OriginError(
                            f"member Content-Length does not match {spec.target}")

                    digest = hashlib.sha256()
                    remaining = length
                    with open(destination, "wb") as output:
                        while remaining > 0:
                            chunk = response.read(min(_IO_CHUNK, remaining))
                            if not chunk:
                                raise OriginError(
                                    f"truncated member {spec.target}")
                            output.write(chunk)
                            digest.update(chunk)
                            remaining -= len(chunk)

                    if digest.hexdigest() != spec.sha256:
                        raise OriginError(
                            f"SHA-256 mismatch for member {spec.target}")
            except OriginError:
                raise
            except (OSError, HTTPException) as exc:
                raise OriginError(f"member GET failed for {spec.target}") from exc
        finally:
            connection.close()

    def _commit_release(self, manifest, stage_dir):
        active_dir = os.path.join(self._root, "active")
        old_dir = os.path.join(self._root, "old")
        replaced_active_dir = False

        if os.path.lexists(active_dir):
            shutil.rmtree(old_dir, ignore_errors=True)
            os.rename(active_dir, old_dir)
            replaced_active_dir = True

        try:
            os.rename(stage_dir, active_dir)
        except OSError:
            if replaced_active_dir:
                os.rename(old_dir, active_dir)
            raise

        members = {}
        for index, spec in enumerate(manifest.members.values()):
            path = os.path.join(active_dir, self._member_filename(index))
            members[spec.target] = _StoredMember(
                spec.target, spec.size, spec.sha256, path)

        new_active = _ActiveRelease(manifest.version, active_dir, members)
        # Serial use means no read can be in progress during this assignment.
        self._active = new_active

        if replaced_active_dir:
            shutil.rmtree(old_dir)
        return manifest.version
