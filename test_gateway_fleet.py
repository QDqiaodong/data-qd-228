"""Tests for in-process gateway rotation and request-version pinning."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from gateway_fleet import FleetBusy, GatewayFleet, NoReadyWorker
from reader import OriginError
from release_origin import ReleaseOrigin


MANIFEST = "/release/manifest.json"
WORKERS = {
    "backup": {"shared"},
    "edge": {"shared", "critical"},
}
MINIMUMS = {"shared": 1, "critical": 1}


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class ScriptedReleaseOrigin:
    """A real loopback origin speaking the ReleaseReader manifest protocol."""

    def __init__(self) -> None:
        self.port: int | None = None
        self._lock = threading.Lock()
        self._gets: list[str] = []
        self._manifests: dict[int, bytes] = {}
        self._bodies: dict[str, bytes] = {}
        self._active: int | None = None
        self._targets: dict[tuple[int, str], str] = {}

        self.gate_target: str | None = None
        self.gate_entered = threading.Event()
        self.gate_release = threading.Event()
        self.gate_failure = False

        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self) -> None:
                with origin._lock:
                    origin._gets.append(self.path)
                    gate_target = origin.gate_target

                if self.path == gate_target:
                    origin.gate_entered.set()
                    # Tests always release this event; the timeout only turns a
                    # broken test into a failure instead of hanging the suite.
                    origin.gate_release.wait(10)
                    with origin._lock:
                        fail = origin.gate_failure
                    if fail:
                        body = b"injected member failure"
                        self.send_response(500)
                        self.send_header("Content-Length", str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return

                with origin._lock:
                    active = origin._active
                    if self.path == MANIFEST and active is not None:
                        body = origin._manifests[active]
                    else:
                        body = origin._bodies.get(self.path)

                if body is None:
                    payload = b"not found"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def __enter__(self) -> "ScriptedReleaseOrigin":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    def publish(self, version: int, members: dict[str, bytes]) -> None:
        specs = []
        with self._lock:
            for name in sorted(members):
                body = members[name]
                target = f"/members/{name}/{digest(body)}"
                self._targets[(version, name)] = target
                self._bodies[target] = body
                specs.append(
                    {
                        "target": target,
                        "size": len(body),
                        "sha256": digest(body),
                    }
                )
            manifest = {
                "version": version,
                "serve_min": 0,
                "members": specs,
            }
            self._manifests[version] = json.dumps(
                manifest, separators=(",", ":")
            ).encode("utf-8")
            self._active = version

    def target(self, version: int, name: str) -> str:
        return self._targets[(version, name)]

    def arm_gate(self, target: str, *, failure: bool = False) -> None:
        with self._lock:
            self.gate_target = target
            self.gate_failure = failure
            self.gate_entered = threading.Event()
            self.gate_release = threading.Event()

    def release_gate(self, *, failure: bool | None = None) -> None:
        with self._lock:
            if failure is not None:
                self.gate_failure = failure
        self.gate_release.set()

    @property
    def gets(self) -> list[str]:
        with self._lock:
            return list(self._gets)

    def clear_gets(self) -> None:
        with self._lock:
            self._gets.clear()

    def close(self) -> None:
        self.gate_release.set()
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(5)


def release_one() -> dict[str, bytes]:
    return {
        "shared.txt": b"shared-v1-bytes",
        "constant.txt": b"constant-bytes",
        "removed.txt": b"removed-v1-bytes",
        "critical.txt": b"critical-v1-bytes",
    }


def release_two() -> dict[str, bytes]:
    return {
        "shared.txt": b"shared-v2-bytes",
        "constant.txt": b"constant-bytes",
        "added.txt": b"added-v2-bytes",
        "critical.txt": b"critical-v2-bytes",
    }


def release_three() -> dict[str, bytes]:
    return {
        "shared.txt": b"shared-v3-bytes",
        "constant.txt": b"constant-bytes",
        "added_later.txt": b"added-v3-bytes",
        "critical.txt": b"critical-v3-bytes",
    }


class GatewayFleetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = tempfile.mkdtemp(prefix="gateway-fleet-test-")
        self.origin = ScriptedReleaseOrigin()
        self.origin.__enter__()
        self.addCleanup(self.origin.close)
        self.fleet: GatewayFleet | None = None

    def tearDown(self) -> None:
        if self.fleet is not None:
            try:
                self.fleet.close()
            except FleetBusy:
                # Individual tests that intentionally keep handles open close
                # them before cleanup; this is a last-resort resource release.
                pass
        shutil.rmtree(self.scratch, ignore_errors=True)

    def make_fleet(self, slots: int = 2) -> GatewayFleet:
        fleet = GatewayFleet(
            "127.0.0.1",
            self.origin.port,
            MANIFEST,
            scratch_dir=self.scratch,
            workers=WORKERS,
            minimums=MINIMUMS,
            slots=slots,
            timeout=10,
        )
        self.fleet = fleet
        return fleet

    def install_both_v1(self, fleet: GatewayFleet) -> None:
        self.origin.publish(1, release_one())
        self.assertEqual(fleet.roll("backup"), 1)
        self.assertEqual(fleet.roll("edge"), 1)
        self.origin.clear_gets()

    def start_roll(self, fleet: GatewayFleet, worker: str):
        outcome: dict[str, object] = {}

        def run() -> None:
            try:
                outcome["version"] = fleet.roll(worker)
            except BaseException as exc:  # captured and asserted by caller
                outcome["error"] = exc

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread, outcome

    def finish_roll(
        self, thread: threading.Thread, outcome: dict[str, object]
    ) -> int:
        thread.join(10)
        self.assertFalse(thread.is_alive(), "roll did not finish")
        if "error" in outcome:
            raise outcome["error"]
        return outcome["version"]  # type: ignore[return-value]

    def test_two_slot_backup_rotation_requires_handle_return_for_in_place(self):
        fleet = self.make_fleet(slots=2)
        self.install_both_v1(fleet)

        pinned = fleet.open_request("shared")
        self.assertEqual((pinned.worker, pinned.version), ("backup", 1))
        removed_target = self.origin.target(1, "removed.txt")
        self.assertEqual(pinned.read(removed_target), b"removed-v1-bytes")

        # Both slots are current contexts; the only possible in-place refresh
        # of backup is forbidden while this request still pins that slot.
        with self.assertRaises(FleetBusy):
            fleet.roll("backup")
        self.assertEqual(self.origin.gets, [])
        self.assertEqual(pinned.read(removed_target), b"removed-v1-bytes")
        pinned.close()

        self.origin.publish(2, release_two())
        added_target = self.origin.target(2, "added.txt")
        self.origin.arm_gate(added_target)
        thread, outcome = self.start_roll(fleet, "backup")
        self.assertTrue(self.origin.gate_entered.wait(10))

        # The backup slot was atomically vacated and is being refreshed in
        # place, so total occupancy remains exactly two.
        self.assertEqual(fleet.occupied_slots, 2)

        during = fleet.open_request("shared")
        self.assertEqual(during.worker, "edge")
        self.assertEqual(during.version, 1)
        self.assertEqual(
            during.read(self.origin.target(1, "shared.txt")),
            b"shared-v1-bytes",
        )
        self.assertEqual(fleet.occupied_slots, 2)

        self.origin.release_gate()
        self.assertEqual(self.finish_roll(thread, outcome), 2)

        after = fleet.open_request("shared")
        self.assertEqual(after.worker, "backup")
        self.assertEqual(after.version, 2)
        self.assertEqual(
            after.read(self.origin.target(2, "shared.txt")),
            b"shared-v2-bytes",
        )
        self.assertEqual(
            after.read(added_target),
            b"added-v2-bytes",
        )

        during.close()
        after.close()

    def test_three_slot_critical_rotation_pins_deleted_member(self):
        fleet = self.make_fleet(slots=3)
        self.install_both_v1(fleet)

        self.origin.publish(2, release_two())
        added_target = self.origin.target(2, "added.txt")
        self.origin.arm_gate(added_target)
        thread, outcome = self.start_roll(fleet, "edge")
        self.assertTrue(self.origin.gate_entered.wait(10))

        # backup current + edge old/current + edge candidate.
        self.assertEqual(fleet.occupied_slots, 3)

        old_request = fleet.open_request("critical")
        self.assertEqual(old_request.worker, "edge")
        self.assertEqual(old_request.version, 1)
        self.assertEqual(
            old_request.read(self.origin.target(1, "critical.txt")),
            b"critical-v1-bytes",
        )

        self.origin.release_gate()
        self.assertEqual(self.finish_roll(thread, outcome), 2)

        # Old edge context remains occupied by old_request.
        self.assertEqual(fleet.occupied_slots, 3)
        new_request = fleet.open_request("critical")
        self.assertEqual(new_request.worker, "edge")
        self.assertEqual(new_request.version, 2)
        self.assertEqual(
            new_request.read(self.origin.target(2, "critical.txt")),
            b"critical-v2-bytes",
        )
        new_request.close()

        # Publish v3 while the v1 request is still open.  All three slots are
        # occupied and vacating the sole critical worker would violate floor 1.
        self.origin.publish(3, release_three())
        self.origin.clear_gets()
        with self.assertRaises(FleetBusy):
            fleet.roll("edge")
        self.assertEqual(self.origin.gets, [])
        self.assertEqual(
            old_request.read(self.origin.target(1, "removed.txt")),
            b"removed-v1-bytes",
        )
        self.assertEqual(fleet.occupied_slots, 3)

        old_request.close()
        self.assertEqual(fleet.occupied_slots, 2)

        self.assertEqual(fleet.roll("edge"), 3)
        self.assertEqual(fleet.occupied_slots, 2)
        latest = fleet.open_request("critical")
        self.assertEqual(latest.version, 3)
        self.assertEqual(
            latest.read(self.origin.target(3, "critical.txt")),
            b"critical-v3-bytes",
        )
        latest.close()

    def test_failed_extra_candidate_restores_old_and_masks_failure_strategy(self):
        fleet = self.make_fleet(slots=3)
        self.install_both_v1(fleet)

        self.origin.publish(2, release_two())
        added_target = self.origin.target(2, "added.txt")
        self.origin.arm_gate(added_target, failure=True)
        thread, outcome = self.start_roll(fleet, "backup")
        self.assertTrue(self.origin.gate_entered.wait(10))

        paused_slots = fleet.occupied_slots
        paused_request = fleet.open_request("shared")

        # The specification permits either keeping backup in traffic with an
        # extra candidate slot, or vacating backup and routing shared to edge.
        if paused_slots == 3:
            self.assertEqual(paused_request.worker, "backup")
            self.assertEqual(paused_request.version, 1)
        elif paused_slots == 2:
            self.assertEqual(paused_request.worker, "edge")
            self.assertEqual(paused_request.version, 1)
        else:  # pragma: no cover - explicit diagnostic for unexpected strategy
            self.fail(f"unexpected paused slot count: {paused_slots}")

        self.assertEqual(
            paused_request.read(self.origin.target(1, "shared.txt")),
            b"shared-v1-bytes",
        )

        self.origin.release_gate(failure=True)
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertIn("error", outcome)
        self.assertIsInstance(outcome["error"], OriginError)

        paused_request.close()
        self.assertEqual(fleet.occupied_slots, 2)

        restored = fleet.open_request("shared")
        self.assertEqual(restored.worker, "backup")
        self.assertEqual(restored.version, 1)
        self.assertEqual(
            restored.read(self.origin.target(1, "shared.txt")),
            b"shared-v1-bytes",
        )
        # The failed candidate's newly downloaded target is not readable from
        # the restored whole release.
        with self.assertRaises(KeyError):
            restored.read(added_target)
        restored.close()

        # The gate event is already released and failure is cleared; retrying
        # the same published version installs it with a fresh candidate reader.
        self.origin.release_gate(failure=False)
        self.assertEqual(fleet.roll("backup"), 2)
        self.assertEqual(fleet.occupied_slots, 2)

        upgraded = fleet.open_request("shared")
        self.assertEqual(upgraded.worker, "backup")
        self.assertEqual(upgraded.version, 2)
        self.assertEqual(
            upgraded.read(self.origin.target(2, "shared.txt")),
            b"shared-v2-bytes",
        )
        upgraded.close()

    def test_failed_in_place_rotation_restores_old_version(self):
        fleet = self.make_fleet(slots=2)
        self.install_both_v1(fleet)

        self.origin.publish(2, release_two())
        added_target = self.origin.target(2, "added.txt")
        self.origin.arm_gate(added_target, failure=True)
        thread, outcome = self.start_roll(fleet, "backup")
        self.assertTrue(self.origin.gate_entered.wait(10))
        self.assertEqual(fleet.occupied_slots, 2)

        during = fleet.open_request("shared")
        self.assertEqual(during.worker, "edge")
        self.assertEqual(during.version, 1)

        self.origin.release_gate(failure=True)
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome.get("error"), OriginError)
        during.close()

        restored = fleet.open_request("shared")
        self.assertEqual((restored.worker, restored.version), ("backup", 1))
        with self.assertRaises(KeyError):
            restored.read(added_target)
        restored.close()

        self.origin.release_gate(failure=False)
        self.assertEqual(fleet.roll("backup"), 2)

    def test_first_install_failure_leaves_no_ready_worker_or_slot(self):
        fleet = self.make_fleet(slots=2)
        self.origin.publish(1, release_one())
        constant_target = self.origin.target(1, "constant.txt")
        self.origin.arm_gate(constant_target, failure=True)

        thread, outcome = self.start_roll(fleet, "backup")
        self.assertTrue(self.origin.gate_entered.wait(10))
        self.origin.release_gate(failure=True)
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome.get("error"), OriginError)

        self.assertEqual(fleet.occupied_slots, 0)
        with self.assertRaises(NoReadyWorker):
            fleet.open_request("shared")
        self.assertEqual(os.listdir(self.scratch), [])

        # The previous release event remains set, so this retry does not block.
        self.origin.release_gate(failure=False)
        self.assertEqual(fleet.roll("backup"), 1)
        self.assertEqual(fleet.occupied_slots, 1)
        handle = fleet.open_request("shared")
        self.assertEqual(handle.worker, "backup")
        handle.close()

    def test_handle_cap_context_and_idempotent_close(self):
        fleet = self.make_fleet(slots=2)
        self.install_both_v1(fleet)

        handles = [fleet.open_request("shared") for _ in range(64)]
        with self.assertRaises(FleetBusy):
            fleet.open_request("shared")

        handles[0].close()
        replacement = fleet.open_request("shared")
        handles.append(replacement)

        handle = handles[1]
        target = self.origin.target(1, "constant.txt")
        self.assertEqual(handle.read(target), b"constant-bytes")
        handle.close()
        handle.close()
        with self.assertRaises(RuntimeError):
            handle.read(target)

        with fleet.open_request("critical") as contextual:
            self.assertEqual(contextual.worker, "edge")
        with self.assertRaises(RuntimeError):
            contextual.read(target)

        for item in handles:
            item.close()

    def test_fleet_close_requires_returned_handles_and_cleans_scratch(self):
        fleet = self.make_fleet(slots=2)
        self.install_both_v1(fleet)
        self.assertNotEqual(os.listdir(self.scratch), [])

        pinned = fleet.open_request("shared")
        with self.assertRaises(FleetBusy):
            fleet.close()
        self.assertNotEqual(os.listdir(self.scratch), [])
        self.assertEqual(pinned.read(self.origin.target(1, "shared.txt")),
                         b"shared-v1-bytes")

        pinned.close()
        fleet.close()
        fleet.close()
        self.assertEqual(os.listdir(self.scratch), [])
        self.assertEqual(fleet.occupied_slots, 0)
        with self.assertRaises(RuntimeError):
            fleet.roll("backup")
        with self.assertRaises(RuntimeError):
            fleet.open_request("shared")
        self.fleet = None

    def test_ascii_ascending_worker_selection(self):
        fleet = GatewayFleet(
            "127.0.0.1",
            self.origin.port,
            MANIFEST,
            scratch_dir=self.scratch,
            workers={
                "alpha": {"g"},
                "Zulu": {"g"},
                "middle": {"g"},
            },
            minimums={"g": 1},
            slots=3,
            timeout=5,
        )
        self.fleet = fleet
        self.origin.publish(1, {"a": b"a"})
        for worker in ("alpha", "Zulu", "middle"):
            self.assertEqual(fleet.roll(worker), 1)

        handle = fleet.open_request("g")
        # Uppercase ASCII letters sort before lowercase letters.
        self.assertEqual(handle.worker, "Zulu")
        handle.close()

    def test_validation_errors_do_not_change_state_or_contact_origin(self):
        fleet = self.make_fleet(slots=2)

        with self.assertRaises(ValueError):
            fleet.roll("missing")
        with self.assertRaises(ValueError):
            fleet.roll("bad name!")
        with self.assertRaises(ValueError):
            fleet.open_request("missing-group")
        with self.assertRaises(ValueError):
            fleet.open_request("bad group!")
        self.assertEqual(self.origin.gets, [])
        self.assertEqual(fleet.occupied_slots, 0)

    def test_constructor_validation(self):
        def construct(**overrides):
            params = dict(
                host="127.0.0.1",
                port=self.origin.port,
                manifest_target=MANIFEST,
                scratch_dir=self.scratch,
                workers=WORKERS,
                minimums=MINIMUMS,
                slots=2,
            )
            params.update(overrides)
            return GatewayFleet(**params)

        with self.assertRaises(ValueError):
            construct(workers={"only": {"g"}})
        with self.assertRaises(ValueError):
            construct(workers={f"w{i}": {"g"} for i in range(9)})
        with self.assertRaises(ValueError):
            construct(workers={"bad!": {"g"}})
        with self.assertRaises(ValueError):
            construct(workers={"w": {"bad!"}})
        with self.assertRaises(ValueError):
            construct(workers={"w": "shared"})
        with self.assertRaises(ValueError):
            construct(
                workers={"backup": {"shared"}, "edge": "shared"}
            )
        with self.assertRaises(ValueError):
            construct(minimums={"shared": 1})  # missing critical
        with self.assertRaises(ValueError):
            construct(minimums={"shared": 1, "critical": 2})  # edge only once
        with self.assertRaises(ValueError):
            construct(minimums={"shared": 0, "critical": 1})
        with self.assertRaises(ValueError):
            construct(minimums={"shared": True, "critical": 1})
        with self.assertRaises(ValueError):
            construct(slots=1)
        with self.assertRaises(ValueError):
            construct(slots=5)
        with self.assertRaises(ValueError):
            construct(slots=True)
        with self.assertRaises(ValueError):
            construct(port=True)
        with self.assertRaises(ValueError):
            construct(manifest_target="not-absolute")
        with self.assertRaises(ValueError):
            construct(scratch_dir="/does/not/exist/gateway-fleet")

    def test_works_with_repository_release_origin(self):
        with ReleaseOrigin(max_bytes=1024 * 1024, max_versions=4) as origin:
            origin.publish(
                10,
                {
                    "shared": b"shared-10",
                    "old": b"old-10",
                },
            )
            fleet = GatewayFleet(
                "127.0.0.1",
                origin.port,
                origin.manifest_target,
                scratch_dir=self.scratch,
                workers={"backup": {"shared"}, "edge": {"shared"}},
                minimums={"shared": 1},
                slots=3,
                timeout=5,
            )
            try:
                self.assertEqual(fleet.roll("backup"), 10)
                self.assertEqual(fleet.roll("edge"), 10)
                handle = fleet.open_request("shared")
                old_target = (
                    "/members/old/" + hashlib.sha256(b"old-10").hexdigest()
                )
                self.assertEqual(handle.read(old_target), b"old-10")

                origin.publish(
                    11,
                    {
                        "shared": b"shared-11",
                        "new": b"new-11",
                    },
                )
                self.assertEqual(fleet.roll("backup"), 11)
                self.assertEqual(handle.version, 10)
                self.assertEqual(handle.read(old_target), b"old-10")
                handle.close()
            finally:
                fleet.close()


if __name__ == "__main__":
    unittest.main()
