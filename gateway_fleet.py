"""In-process rolling installation and request-version pinning.

A :class:`GatewayFleet` owns one or more named gateway workers.  Each running
slot is an independent :class:`~reader.ReleaseReader` context.  Request handles
retain the slot containing the whole release they selected, allowing later
local reads to stay on that exact release after the worker's routing pointer
has moved to a new version.
"""
from __future__ import annotations

from collections.abc import Mapping
import math
import os
import re
import threading
from typing import Any

try:  # Support both in-tree test execution and package-style imports.
    from .reader import (
        OriginError,
        ReleaseReader,
        _validate_release_target,
    )
except ImportError:  # pragma: no cover - top-level module layout
    from reader import (  # type: ignore
        OriginError,
        ReleaseReader,
        _validate_release_target,
    )

__all__ = [
    "FleetBusy",
    "GatewayFleet",
    "NoReadyWorker",
    "OriginError",
    "RequestHandle",
]


_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
_MAX_OPEN_HANDLES = 64


class FleetBusy(RuntimeError):
    """No slot/request capacity permits the requested operation right now."""


class NoReadyWorker(RuntimeError):
    """The group is known, but no supporting worker can take a new request."""


def _strict_int(value: Any, name: str, lower: int, upper: int) -> int:
    # ``type(value) is int`` deliberately rejects bool, which is an int
    # subclass but is not a valid version/capacity integer.
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(f"{name} must be an integer in [{lower}, {upper}]")
    return value


def _validate_name(value: Any, label: str) -> str:
    if type(value) is not str or _NAME_RE.fullmatch(value) is None:
        raise ValueError(
            f"{label} must be 1..32 ASCII letters, digits, '_' or '-'"
        )
    return value


def _normalize_groups(value: Any, worker: str) -> frozenset[str]:
    # A string/bytes is iterable, but is a single group name rather than a
    # collection of groups.  Reject it before frozenset() splits it.
    if isinstance(value, (str, bytes)):
        raise ValueError(
            f"worker {worker!r} must map to a non-empty collection of groups"
        )

    try:
        groups = frozenset(value)
    except TypeError:
        raise ValueError(
            f"worker {worker!r} must map to a non-empty collection of groups"
        ) from None

    if not groups:
        raise ValueError(f"worker {worker!r} must support at least one group")
    for group in groups:
        _validate_name(group, "group name")
    return groups


class _Slot:
    """One occupied runtime slot and its independent ReleaseReader."""

    __slots__ = (
        "worker",
        "reader",
        "version",
        "refs",
        "accepting",
        "draining",
        "closed",
    )

    def __init__(
        self,
        worker: str,
        host: str,
        port: int,
        manifest_target: str,
        scratch_dir: str,
        timeout: float | int | None,
    ) -> None:
        self.worker = worker
        # Construction creates this reader's private scratch subtree but does
        # not contact the origin.
        self.reader = ReleaseReader(
            host,
            port,
            manifest_target,
            scratch_dir=scratch_dir,
            timeout=timeout,
        )
        self.version: int | None = None
        self.refs = 0
        self.accepting = False
        self.draining = False
        self.closed = False

    def read(self, target: str) -> bytes:
        return self.reader.read_active(target)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.reader.close()


class RequestHandle:
    """A request pinned to the exact release selected by ``open_request``."""

    __slots__ = (
        "_fleet",
        "_slot",
        "_worker",
        "_version",
        "_closed",
    )

    def __init__(
        self,
        fleet: "GatewayFleet",
        slot: _Slot,
        worker: str,
        version: int,
    ) -> None:
        self._fleet = fleet
        self._slot = slot
        self._worker = worker
        self._version = version
        self._closed = False

    @property
    def worker(self) -> str:
        return self._worker

    @property
    def version(self) -> int:
        return self._version

    def read(self, target: str) -> bytes:
        if self._closed:
            raise RuntimeError("request handle is closed")
        # The slot is retained by fleet slot accounting until close(); all
        # reads are therefore local and cannot observe a later rotation.
        return self._slot.read(target)

    def close(self) -> None:
        if self._closed:
            return
        self._fleet._close_handle(self)

    def __enter__(self) -> "RequestHandle":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self.close()
        return False


class GatewayFleet:
    """Coordinate rolling installs, slot capacity, routing, and request pins."""

    def __init__(
        self,
        host: str,
        port: int,
        manifest_target: str,
        *,
        scratch_dir: str | os.PathLike[str],
        workers: Mapping[str, Any],
        minimums: Mapping[str, int],
        slots: int,
        timeout: float | int | None = 2,
    ) -> None:
        if not isinstance(workers, Mapping):
            raise ValueError("workers must be a dictionary")
        _strict_int(len(workers), "number of workers", 2, 8)

        normalized_workers: dict[str, frozenset[str]] = {}
        for worker, groups in workers.items():
            _validate_name(worker, "worker name")
            if worker in normalized_workers:  # pragma: no cover - mappings unique
                raise ValueError(f"duplicate worker name {worker!r}")
            normalized_workers[worker] = _normalize_groups(groups, worker)

        all_groups = frozenset().union(*normalized_workers.values())
        _strict_int(len(all_groups), "number of groups", 1, 8)

        if not isinstance(minimums, Mapping):
            raise ValueError("minimums must be a mapping")
        if frozenset(minimums) != all_groups:
            raise ValueError(
                "minimums must specify exactly every configured traffic group"
            )

        normalized_minimums: dict[str, int] = {}
        for group, minimum in minimums.items():
            _validate_name(group, "group name")
            supporters = sum(
                group in groups for groups in normalized_workers.values()
            )
            normalized_minimums[group] = _strict_int(
                minimum, f"minimum for group {group!r}", 1, supporters
            )

        worker_count = len(normalized_workers)
        slot_limit = _strict_int(
            slots, "slots", worker_count, worker_count + 2
        )

        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        _strict_int(port, "port", 1, 65535)
        _validate_release_target(manifest_target)

        if timeout is not None:
            if (
                type(timeout) is bool
                or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout)
                or timeout <= 0
            ):
                raise ValueError(
                    "timeout must be a positive finite number or None"
                )

        try:
            scratch_path = os.path.abspath(os.fspath(scratch_dir))
        except (TypeError, ValueError) as exc:
            raise ValueError("scratch_dir must be a filesystem path") from exc
        if not os.path.isdir(scratch_path):
            raise ValueError("scratch_dir must be an existing directory")

        # Validation is complete.  Do not create a slot/ReleaseReader yet:
        # construction does not contact the origin and has no occupied slots.
        self._host = host
        self._port = port
        self._manifest_target = manifest_target
        self._timeout = timeout
        self._workers = normalized_workers
        self._groups = all_groups
        self._minimums = normalized_minimums
        self._slot_limit = slot_limit
        self._scratch_dir = scratch_path
        self._lock = threading.RLock()
        self._roll_lock = threading.Lock()
        self._closed = False
        self._slots: set[_Slot] = set()
        self._current: dict[str, _Slot | None] = {
            worker: None for worker in normalized_workers
        }
        self._open_handles = 0

    @property
    def occupied_slots(self) -> int:
        with self._lock:
            return len(self._slots)

    def __enter__(self) -> "GatewayFleet":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self.close()
        return False

    def close(self) -> None:
        # Serial management calls mean a roll and close are not expected to
        # overlap.  Acquire non-blockingly rather than tearing down a slot
        # whose HTTP installation is still running.
        if not self._roll_lock.acquire(blocking=False):
            raise FleetBusy("a roll is in progress")
        try:
            slots_to_close: list[_Slot] = []
            with self._lock:
                if self._closed:
                    return
                if self._open_handles:
                    raise FleetBusy(
                        "cannot close GatewayFleet while request handles are open"
                    )
                self._closed = True
                slots_to_close = list(self._slots)
                self._slots.clear()
                for worker in self._current:
                    self._current[worker] = None

            for slot in slots_to_close:
                slot.close()
        finally:
            self._roll_lock.release()

    def roll(self, worker: str) -> int:
        with self._roll_lock:
            plan = self._plan_roll(worker)
            candidate = plan["slot"]
            try:
                version = candidate.reader.refresh()
            except BaseException:
                self._fail_roll(plan)
                raise
            else:
                return self._commit_roll(plan, version)

    def open_request(self, group: str) -> RequestHandle:
        with self._lock:
            self._require_open_locked()
            _validate_name(group, "group name")
            if group not in self._groups:
                raise ValueError(f"unknown traffic group {group!r}")

            # The global admission limit is independent of worker/slot count.
            if self._open_handles >= _MAX_OPEN_HANDLES:
                raise FleetBusy("at most 64 request handles may be open")

            for worker in sorted(self._workers):
                if group not in self._workers[worker]:
                    continue
                slot = self._current[worker]
                if slot is None or slot.closed or not slot.accepting:
                    continue
                if slot.version is None:
                    continue

                handle = RequestHandle(
                    self, slot, worker, slot.version
                )
                slot.refs += 1
                self._open_handles += 1
                return handle

            raise NoReadyWorker(f"no ready worker can serve group {group!r}")

    # ------------------------------------------------------------------
    # Internal state transitions
    # ------------------------------------------------------------------

    def _require_open_locked(self) -> None:
        if self._closed:
            raise RuntimeError("GatewayFleet is closed")

    def _plan_roll(self, worker: str) -> dict[str, Any]:
        with self._lock:
            self._require_open_locked()
            _validate_name(worker, "worker name")
            if worker not in self._workers:
                raise ValueError(f"unknown worker {worker!r}")

            current = self._current[worker]

            # Preferred path: keep the old context in traffic while a wholly
            # independent candidate downloads into a new occupied slot.
            if len(self._slots) < self._slot_limit:
                candidate = _Slot(
                    worker,
                    self._host,
                    self._port,
                    self._manifest_target,
                    self._scratch_dir,
                    self._timeout,
                )
                self._slots.add(candidate)
                return {
                    "mode": "new-slot",
                    "worker": worker,
                    "slot": candidate,
                    "old": current,
                }

            # No additional slot exists.  In-place refresh is legal only when
            # the exact current slot has no checked-out requests and removing
            # this worker from new-request routing cannot reduce service below
            # min(configured minimum, availability at roll start).
            if (
                current is None
                or current.closed
                or not current.accepting
                or current.refs != 0
            ):
                raise FleetBusy(
                    "no roll slot is available without breaking requests"
                )

            available_at_start = self._available_counts_locked()
            for group in self._workers[worker]:
                start_count = available_at_start[group]
                protected = min(self._minimums[group], start_count)
                after_withdrawal = start_count - 1
                if after_withdrawal < protected:
                    raise FleetBusy(
                        f"rotating {worker!r} would reduce {group!r} service "
                        "below its protected availability"
                    )

            # This assignment and all checks above are atomic with respect to
            # open_request(), so no new handle can join this slot afterwards.
            current.accepting = False
            return {
                "mode": "in-place",
                "worker": worker,
                "slot": current,
                "old": None,
            }

    def _available_counts_locked(self) -> dict[str, int]:
        counts = {group: 0 for group in self._groups}
        for worker, groups in self._workers.items():
            slot = self._current[worker]
            if (
                slot is not None
                and not slot.closed
                and slot.accepting
                and slot.version is not None
            ):
                for group in groups:
                    counts[group] += 1
        return counts

    def _fail_roll(self, plan: Mapping[str, Any]) -> None:
        slots_to_close: list[_Slot] = []
        with self._lock:
            if plan["mode"] == "new-slot":
                candidate = plan["slot"]
                self._slots.discard(candidate)
                slots_to_close.append(candidate)
            else:
                # The old whole release remains active in this ReleaseReader
                # because the origin contract keeps serve_min at zero.
                plan["slot"].accepting = True

        for slot in slots_to_close:
            slot.close()

    def _commit_roll(self, plan: Mapping[str, Any], version: int) -> int:
        slots_to_close: list[_Slot] = []
        with self._lock:
            self._require_open_locked()
            worker = plan["worker"]
            candidate = plan["slot"]
            candidate.version = version

            if plan["mode"] == "new-slot":
                old = plan["old"]
                candidate.accepting = True
                self._current[worker] = candidate

                if old is not None:
                    old.accepting = False
                    if old.refs == 0:
                        old.draining = False
                        self._slots.discard(old)
                        slots_to_close.append(old)
                    else:
                        # Keep the old ReleaseReader and its files alive until
                        # the final request handle is returned.
                        old.draining = True
            else:
                candidate.accepting = True
                candidate.draining = False

        # Filesystem removal does not need to block request admission.  Slot
        # accounting above has already made the reclamation visible.
        for slot in slots_to_close:
            slot.close()
        return version

    def _close_handle(self, handle: RequestHandle) -> None:
        slot_to_close: _Slot | None = None
        with self._lock:
            if handle._closed:
                return
            handle._closed = True
            self._open_handles -= 1

            slot = handle._slot
            slot.refs -= 1
            if slot.refs < 0:  # pragma: no cover - defensive invariant
                raise RuntimeError("request handle accounting is corrupted")

            # A retired context is reclaimed immediately when its last pinned
            # request is returned.  Current contexts remain in their slot.
            if slot.draining and slot.refs == 0:
                self._slots.discard(slot)
                slot_to_close = slot

        if slot_to_close is not None:
            slot_to_close.close()
