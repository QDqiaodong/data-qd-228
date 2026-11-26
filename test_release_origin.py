"""Standard-library tests for the in-process managed ReleaseOrigin.

Every scenario is driven over real loopback HTTP.  Whole releases are
installed with the production ``ReleaseReader``; member bytes are also fetched
with raw GETs so the manifest/target/size/SHA-256 protocol is exercised rather
than only management return values.
"""
import hashlib
from http.client import HTTPConnection
import json
import shutil
import socket
import tempfile
import unittest

from reader import OriginError, ReleaseReader, ReleaseUnavailable
from release_origin import ReleaseCapacityError, ReleaseOrigin


MANIFEST = "/release/manifest.json"
KIB = 1024
MIB = 1024 * 1024


def sha256_bytes(body):
    return hashlib.sha256(body).hexdigest()


def member_target(name, body):
    return f"/members/{name}/{sha256_bytes(body)}"


def http_get(port, path, timeout=10):
    connection = HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request("GET", path, headers={"Connection": "close"})
        with connection.getresponse() as response:
            headers = dict(response.getheaders())
            return response.status, headers, response.read()
    finally:
        connection.close()


class OriginTestCase(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.mkdtemp(prefix="release-origin-test-")

    def tearDown(self):
        shutil.rmtree(self.scratch, ignore_errors=True)

    def make_reader(self, origin):
        reader = ReleaseReader(
            "127.0.0.1", origin.port, origin.manifest_target,
            scratch_dir=self.scratch, timeout=10)
        self.addCleanup(reader.close)
        return reader

    def raw(self, origin, path):
        """Status and body only; response headers vary per request."""
        status, _headers, body = http_get(origin.port, path)
        return status, body

    def assert_member(self, origin, target, expected):
        status, headers, body = http_get(origin.port, target)
        self.assertEqual(status, 200)
        self.assertEqual(int(headers["Content-Length"]), len(expected))
        self.assertEqual(body, expected)

    def assert_missing(self, origin, target):
        status, _headers, _body = http_get(origin.port, target)
        self.assertEqual(status, 404)

    def manifest(self, origin):
        status, _headers, body = http_get(origin.port, MANIFEST)
        self.assertEqual(status, 200)
        return json.loads(body)


class ReleaseOriginProtocolTest(OriginTestCase):
    def test_manifest_member_targets_and_immutable_copies(self):
        with ReleaseOrigin(max_bytes=4 * MIB, max_versions=8) as origin:
            self.assertIsInstance(origin.port, int)
            self.assertGreater(origin.port, 0)
            self.assertEqual(origin.manifest_target, MANIFEST)
            self.assertIsNone(origin.active_version)
            self.assertEqual(origin.retained_versions(), ())
            self.assertEqual(self.raw(origin, MANIFEST), (404, b""))

            # The production reader itself cannot install anything yet.
            reader = self.make_reader(origin)
            self.assertIsNone(reader.active_version)
            with self.assertRaises(OriginError):
                reader.refresh()
            with self.assertRaises(ReleaseUnavailable):
                reader.read_active("/anything")

            body = b"configuration-v1"
            target = member_target("cfg.v1", body)
            empty_target = member_target("empty", b"")
            # Unpublished bytes are not reachable early.
            self.assert_missing(origin, target)
            self.assert_missing(origin, empty_target)

            self.assertEqual(
                origin.publish(1, {"cfg.v1": body, "empty": b""}), 1)
            self.assertEqual(origin.active_version, 1)
            self.assertEqual(origin.retained_versions(), (1,))

            status, headers, raw = http_get(origin.port, MANIFEST)
            self.assertEqual(status, 200)
            self.assertEqual(int(headers["Content-Length"]), len(raw))
            manifest = json.loads(raw)
            self.assertEqual(manifest["version"], 1)
            self.assertEqual(manifest["serve_min"], 0)
            declared = {member["target"]: member
                        for member in manifest["members"]}
            self.assertEqual(set(declared), {target, empty_target})
            self.assertEqual(
                declared[target],
                {"target": target, "size": len(body),
                 "sha256": sha256_bytes(body)})
            self.assertEqual(
                declared[empty_target],
                {"target": empty_target, "size": 0,
                 "sha256": sha256_bytes(b"")})

            self.assert_member(origin, target, body)
            status, headers, empty = http_get(origin.port, empty_target)
            self.assertEqual(
                (status, headers["Content-Length"], empty), (200, "0", b""))

            # Same logical name with identical bytes keeps its target.
            origin.publish(2, {"cfg.v1": body})
            self.assert_member(origin, target, body)

            # Same logical name with different bytes gets a new target; the
            # previous release stays as the rollback point.
            changed = b"configuration-v3"
            changed_target = member_target("cfg.v1", changed)
            origin.publish(3, {"cfg.v1": changed})
            self.assertNotEqual(changed_target, target)
            self.assert_member(origin, changed_target, changed)
            self.assert_member(origin, target, body)

            # Mutating the caller's dictionary after publish changes nothing.
            caller = {"one": b"1" * 10, "two": b"2" * 20}
            origin.publish(4, caller)
            caller.clear()
            caller["three"] = b"3" * 30
            manifest = self.manifest(origin)
            self.assertEqual(manifest["version"], 4)
            self.assertEqual(
                {member["target"] for member in manifest["members"]},
                {member_target("one", b"1" * 10),
                 member_target("two", b"2" * 20)})
            self.assert_member(origin, member_target("one", b"1" * 10),
                               b"1" * 10)
            self.assert_member(origin, member_target("two", b"2" * 20),
                               b"2" * 20)

            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 4)
            self.assertEqual(
                reader.read_active(member_target("one", b"1" * 10)),
                b"1" * 10)
            with self.assertRaises(KeyError):
                reader.read_active(changed_target)

    def test_member_count_and_size_limits(self):
        with ReleaseOrigin(max_bytes=64 * MIB, max_versions=8) as origin:
            # Exactly 64 members, mixing the empty and 1 MiB boundaries.
            members = {"empty": b"", "max": b"x" * MIB}
            members.update({f"m{index}": bytes([index]) * index
                            for index in range(62)})
            self.assertEqual(len(members), 64)
            origin.publish(1, members)
            manifest = self.manifest(origin)
            self.assertEqual(len(manifest["members"]), 64)
            self.assert_member(origin, member_target("empty", b""), b"")
            self.assert_member(origin, member_target("max", b"x" * MIB),
                               b"x" * MIB)

            # The production reader installs the whole 64-member release.
            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 1)
            for name, body in members.items():
                self.assertEqual(
                    reader.read_active(member_target(name, body)), body)

            # A 65th member or a 1 MiB + 1 byte value is rejected before any
            # version number or state is touched.
            with self.assertRaises(ValueError):
                origin.publish(2, {f"n{index}": b"" for index in range(65)})
            with self.assertRaises(ValueError):
                origin.publish(2, {"big": b"x" * (MIB + 1)})
            self.assertEqual(origin.active_version, 1)
            self.assertEqual(origin.retained_versions(), (1,))

    def test_close_releases_listener_and_rejects_management(self):
        origin = ReleaseOrigin(max_bytes=KIB, max_versions=2)
        with origin:
            port = origin.port
            origin.publish(1, {"a": b"a"})
            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 1)

        origin.close()
        origin.close()
        with self.assertRaises(RuntimeError):
            origin.publish(2, {"b": b"b"})
        with self.assertRaises(RuntimeError):
            origin.hold(1)
        with self.assertRaises(RuntimeError):
            origin.unhold(1)
        with self.assertRaises(RuntimeError):
            origin.restore(1, 2)
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=1).close()

    def test_version_count_quota_applies_before_bytes(self):
        bodies = {name: name.encode("ascii") * 8 for name in "abcd"}
        with ReleaseOrigin(max_bytes=MIB, max_versions=2) as origin:
            origin.publish(1, {"a": bodies["a"]})
            origin.publish(2, {"b": bodies["b"]})
            origin.publish(3, {"c": bodies["c"]})
            self.assertEqual(origin.retained_versions(), (2, 3))
            self.assert_missing(origin, member_target("a", bodies["a"]))
            self.assert_member(origin, member_target("c", bodies["c"]),
                               bodies["c"])

            # Bytes are plentiful; protecting the non-current rollback point
            # as well as current plus new would exceed max_versions.
            origin.hold(2)
            d_target = member_target("d", bodies["d"])
            with self.assertRaises(ReleaseCapacityError):
                origin.publish(4, {"d": bodies["d"]})
            self.assertEqual(origin.active_version, 3)
            self.assertEqual(origin.retained_versions(), (2, 3))
            self.assert_missing(origin, d_target)
            self.assert_member(origin, member_target("c", bodies["c"]),
                               bodies["c"])

            origin.unhold(2)
            self.assertEqual(origin.publish(4, {"d": bodies["d"]}), 4)
            self.assertEqual(origin.active_version, 4)
            self.assertEqual(origin.retained_versions(), (3, 4))
            self.assert_missing(origin, member_target("b", bodies["b"]))
            self.assert_member(origin, d_target, bodies["d"])

    def test_selected_in_flight_member_response_survives_cleanup(self):
        big = bytes(range(256)) * 4096  # exactly 1 MiB
        self.assertEqual(len(big), MIB)
        with ReleaseOrigin(max_bytes=MIB + 1, max_versions=8) as origin:
            origin.publish(1, {"big": big})
            target = member_target("big", big)

            connection = socket.create_connection(
                ("127.0.0.1", origin.port), timeout=10)
            self.addCleanup(connection.close)
            connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * KIB)
            connection.sendall(
                f"GET {target} HTTP/1.0\r\n\r\n".encode("ascii"))

            # Read only the status and headers, leaving the body unread.
            header = b""
            while b"\r\n\r\n" not in header:
                chunk = connection.recv(1)
                self.assertTrue(chunk)
                header += chunk
            head = header.split(b"\r\n\r\n", 1)[0].split(b"\r\n")
            self.assertEqual(head[0], b"HTTP/1.0 200 OK")
            lengths = [
                line.split(b":", 1)[1].strip() for line in head
                if line.lower().startswith(b"content-length")]
            self.assertEqual(lengths, [str(MIB).encode("ascii")])

            # The response was selected from release 1.  Release 2 keeps 1 as
            # its rollback point; release 3 retires 1 and frees its bytes.
            origin.publish(2, {"small": b"s"})        # 1 + 1 byte fits
            origin.publish(3, {"small": b"ss"})       # 1 must be evicted
            self.assertEqual(origin.active_version, 3)
            self.assertEqual(origin.retained_versions(), (2, 3))
            self.assert_missing(origin, target)

            received = header.split(b"\r\n\r\n", 1)[1]
            while len(received) < len(big):
                chunk = connection.recv(
                    min(64 * KIB, len(big) - len(received)))
                self.assertTrue(chunk)
                received += chunk
            self.assertEqual(received, big)


FIXTURE = {
    "S": b"S" * (3 * KIB),
    "A": b"A" * (2 * KIB),
    "B": b"B" * (3 * KIB),
    "C": b"C" * (2 * KIB),
    "D": b"D" * (4 * KIB),
}
TARGET = {name: member_target(name, body) for name, body in FIXTURE.items()}


def publish_ten_eleven_twelve(origin):
    """10 (S,A), 11 (S,B), 12 (C): 10 distinct KiB, all retained."""
    origin.publish(10, {"S": FIXTURE["S"], "A": FIXTURE["A"]})
    origin.publish(11, {"S": FIXTURE["S"], "B": FIXTURE["B"]})
    origin.publish(12, {"C": FIXTURE["C"]})
    assert origin.retained_versions() == (10, 11, 12)


class RetentionScenarioTest(OriginTestCase):
    def test_no_marks_evicts_one_of_ten_or_eleven_but_keeps_twelve(self):
        with ReleaseOrigin(max_bytes=12 * KIB, max_versions=8) as origin:
            publish_ten_eleven_twelve(origin)
            self.assertEqual(origin.publish(13, {"D": FIXTURE["D"]}), 13)
            retained = origin.retained_versions()
            # 12 must survive; exactly one of 10/11 gives up its bytes.
            self.assertIn(12, retained)
            self.assertIn(13, retained)
            self.assertEqual(len(retained), 3)
            self.assertNotEqual(10 in retained, 11 in retained)
            self.assert_member(origin, TARGET["D"], FIXTURE["D"])
            self.assert_member(origin, TARGET["S"], FIXTURE["S"])
            surviving = TARGET["A"] if 10 in retained else TARGET["B"]
            evicted = TARGET["B"] if 10 in retained else TARGET["A"]
            self.assert_member(origin, surviving,
                               FIXTURE["A" if 10 in retained else "B"])
            self.assert_missing(origin, evicted)

    def test_holding_ten_evicts_eleven_and_restores_ten_for_gateway(self):
        with ReleaseOrigin(max_bytes=12 * KIB, max_versions=8) as origin:
            publish_ten_eleven_twelve(origin)
            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 12)

            origin.hold(10)
            self.assertEqual(origin.hold(10), None)  # idempotent
            self.assertEqual(origin.publish(13, {"D": FIXTURE["D"]}), 13)
            self.assertEqual(origin.retained_versions(), (10, 12, 13))
            self.assertEqual(origin.active_version, 13)

            # 11's unique member is gone; 10's survives through the hold and
            # the shared member is still reachable with its original bytes.
            self.assert_missing(origin, TARGET["B"])
            self.assert_member(origin, TARGET["A"], FIXTURE["A"])
            self.assert_member(origin, TARGET["S"], FIXTURE["S"])
            self.assert_member(origin, TARGET["C"], FIXTURE["C"])
            self.assert_member(origin, TARGET["D"], FIXTURE["D"])

            self.assertEqual(reader.refresh(), 13)
            self.assertEqual(reader.read_active(TARGET["D"]), FIXTURE["D"])
            with self.assertRaises(KeyError):
                reader.read_active(TARGET["A"])

            # 11 is no longer a usable rollback point.
            with self.assertRaises(KeyError):
                origin.restore(11, 14)
            self.assertEqual(origin.active_version, 13)

            # The saved old targets tell which rollback point still exists.
            self.assertEqual(self.raw(origin, TARGET["A"])[0], 200)
            self.assertEqual(self.raw(origin, TARGET["B"])[0], 404)

            # Republish 10's original configuration as 14; the same gateway
            # reader that installed 13 installs 14 and reads the old bytes.
            self.assertEqual(origin.restore(10, 14), 14)
            self.assertEqual(origin.active_version, 14)
            self.assertEqual(origin.retained_versions(), (10, 12, 13, 14))
            self.assertEqual(reader.refresh(), 14)
            self.assertEqual(reader.read_active(TARGET["S"]), FIXTURE["S"])
            self.assertEqual(reader.read_active(TARGET["A"]), FIXTURE["A"])
            with self.assertRaises(KeyError):
                reader.read_active(TARGET["D"])
            manifest = self.manifest(origin)
            self.assertEqual(manifest["version"], 14)
            self.assertEqual(manifest["serve_min"], 0)
            self.assertEqual(
                {member["target"] for member in manifest["members"]},
                {TARGET["S"], TARGET["A"]})

            # Clearing the mark cleans nothing by itself.
            origin.unhold(10)
            origin.unhold(10)
            self.assertEqual(origin.retained_versions(), (10, 12, 13, 14))
            self.assertEqual(origin.active_version, 14)
            self.assert_member(origin, TARGET["A"], FIXTURE["A"])

    def test_holding_eleven_evicts_ten_and_restores_eleven(self):
        with ReleaseOrigin(max_bytes=12 * KIB, max_versions=8) as origin:
            publish_ten_eleven_twelve(origin)
            origin.hold(11)
            self.assertEqual(origin.publish(13, {"D": FIXTURE["D"]}), 13)
            self.assertEqual(origin.retained_versions(), (11, 12, 13))
            self.assert_missing(origin, TARGET["A"])
            self.assert_member(origin, TARGET["B"], FIXTURE["B"])
            self.assert_member(origin, TARGET["S"], FIXTURE["S"])

            with self.assertRaises(KeyError):
                origin.restore(10, 14)
            self.assertEqual(origin.active_version, 13)

            reader = self.make_reader(origin)
            self.assertEqual(origin.restore(11, 14), 14)
            self.assertEqual(origin.retained_versions(), (11, 12, 13, 14))
            self.assertEqual(reader.refresh(), 14)
            self.assertEqual(reader.read_active(TARGET["S"]), FIXTURE["S"])
            self.assertEqual(reader.read_active(TARGET["B"]), FIXTURE["B"])

    def test_capacity_rejection_is_atomic_then_retry_succeeds(self):
        with ReleaseOrigin(max_bytes=12 * KIB, max_versions=8) as origin:
            publish_ten_eleven_twelve(origin)
            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 12)

            origin.hold(10)
            origin.hold(11)
            before_manifest = self.raw(origin, MANIFEST)
            before_members = {
                name: self.raw(origin, TARGET[name]) for name in "SABC"}

            # 10, 11, 12 are all mandatory plus the new 13: 14 KiB, and no
            # historical version may be dropped to make room.
            with self.assertRaises(ReleaseCapacityError):
                origin.publish(13, {"D": FIXTURE["D"]})

            self.assertEqual(origin.active_version, 12)
            self.assertEqual(origin.retained_versions(), (10, 11, 12))
            self.assertEqual(self.raw(origin, MANIFEST), before_manifest)
            for name in "SABC":
                self.assertEqual(
                    self.raw(origin, TARGET[name]), before_members[name])
            self.assert_missing(origin, TARGET["D"])
            # Rejection consumed no version number and no gateway state.
            self.assertEqual(reader.refresh(), 12)
            self.assertEqual(reader.read_active(TARGET["C"]), FIXTURE["C"])

            # Only 11 is unheld: 10, 12 and 13 coexist at 11 KiB, so the very
            # same 13 now switches successfully and 10 stays rollbackable.
            origin.unhold(11)
            self.assertEqual(origin.publish(13, {"D": FIXTURE["D"]}), 13)
            self.assertEqual(origin.active_version, 13)
            self.assertEqual(origin.retained_versions(), (10, 12, 13))
            self.assert_member(origin, TARGET["A"], FIXTURE["A"])
            self.assert_missing(origin, TARGET["B"])
            self.assert_member(origin, TARGET["S"], FIXTURE["S"])
            self.assert_member(origin, TARGET["D"], FIXTURE["D"])

            self.assertEqual(reader.refresh(), 13)
            self.assertEqual(reader.read_active(TARGET["D"]), FIXTURE["D"])
            # 10 is a rollback point on the origin, not part of the manifest
            # the gateway just installed.
            with self.assertRaises(KeyError):
                reader.read_active(TARGET["A"])

    def test_shared_member_freed_only_after_both_holders_are_dropped(self):
        shared = b"s" * (5 * KIB)
        current_extra = b"c" * (4 * KIB)
        new_extra = b"d" * (4 * KIB)
        shared_target = member_target("shared", shared)
        current_target = member_target("c", current_extra)
        new_target = member_target("d", new_extra)

        with ReleaseOrigin(max_bytes=10 * KIB, max_versions=8) as origin:
            # Versions 1 and 2 jointly reference the 5 KiB shared member.
            origin.publish(1, {"shared": shared})
            origin.publish(2, {"shared": shared})
            # The current version carries its own 4 KiB member.
            origin.publish(3, {"c": current_extra})
            self.assertEqual(origin.retained_versions(), (1, 2, 3))
            self.assert_member(origin, shared_target, shared)
            self.assert_member(origin, current_target, current_extra)

            # Marking either sharing version pins the 5 KiB member next to
            # the new 4 KiB member and the current 4 KiB member: 13 KiB, and
            # deleting only one holder still leaves 13 KiB.
            for held in (1, 2):
                with self.subTest(held=held):
                    origin.hold(held)
                    before = self.raw(origin, MANIFEST)
                    before_members = {
                        shared_target: self.raw(origin, shared_target),
                        current_target: self.raw(origin, current_target),
                    }
                    with self.assertRaises(ReleaseCapacityError):
                        origin.publish(4, {"d": new_extra})
                    self.assertEqual(origin.active_version, 3)
                    self.assertEqual(origin.retained_versions(), (1, 2, 3))
                    self.assertEqual(self.raw(origin, MANIFEST), before)
                    for path, value in before_members.items():
                        self.assertEqual(self.raw(origin, path), value)
                    self.assert_missing(origin, new_target)
                    origin.unhold(held)

            # Unmarked, both sharing versions must be dropped: current 4 KiB
            # plus new 4 KiB fits 10 KiB only once shared's 5 KiB is gone.
            self.assertEqual(origin.publish(4, {"d": new_extra}), 4)
            self.assertEqual(origin.active_version, 4)
            self.assertEqual(origin.retained_versions(), (3, 4))
            self.assert_missing(origin, shared_target)
            self.assert_member(origin, current_target, current_extra)
            self.assert_member(origin, new_target, new_extra)
            manifest = self.manifest(origin)
            self.assertEqual(manifest["version"], 4)
            self.assertEqual(
                {member["target"] for member in manifest["members"]},
                {new_target})

    def test_failed_restore_leaves_current_release_and_catalogue(self):
        with ReleaseOrigin(max_bytes=4 * MIB, max_versions=3) as origin:
            origin.publish(1, {"a": b"one"})
            origin.publish(2, {"b": b"two"})
            origin.publish(3, {"c": b"three"})
            self.assertEqual(origin.retained_versions(), (1, 2, 3))
            origin.hold(1)
            origin.hold(2)

            before_manifest = self.raw(origin, MANIFEST)
            before_members = {
                member_target(name, body): self.raw(
                    origin, member_target(name, body))
                for name, body in
                (("a", b"one"), ("b", b"two"), ("c", b"three"))}

            # Source 1, current 3, new 4 and the two holds need four version
            # slots; only three fit, so the restore is rejected atomically.
            with self.assertRaises(ReleaseCapacityError):
                origin.restore(1, 4)
            self.assertEqual(origin.active_version, 3)
            self.assertEqual(origin.retained_versions(), (1, 2, 3))
            self.assertEqual(self.raw(origin, MANIFEST), before_manifest)
            for path, value in before_members.items():
                self.assertEqual(self.raw(origin, path), value)

            # Releasing one hold lets the unheld version 2 drop; source 1,
            # current 3 and new 4 then fit.
            origin.unhold(2)
            self.assertEqual(origin.restore(1, 4), 4)
            self.assertEqual(origin.active_version, 4)
            self.assertEqual(origin.retained_versions(), (1, 3, 4))
            self.assertEqual(
                self.raw(origin, member_target("a", b"one")), (200, b"one"))
            self.assertEqual(
                self.raw(origin, member_target("c", b"three")), (200, b"three"))
            self.assertEqual(
                self.raw(origin, member_target("b", b"two")), (404, b""))

            reader = self.make_reader(origin)
            self.assertEqual(reader.refresh(), 4)
            self.assertEqual(
                reader.read_active(member_target("a", b"one")), b"one")

    def test_restore_does_not_inherit_the_source_mark(self):
        with ReleaseOrigin(max_bytes=4 * MIB, max_versions=3) as origin:
            origin.publish(1, {"a": b"one"})
            origin.publish(2, {"b": b"two"})
            origin.hold(1)
            self.assertEqual(origin.restore(1, 3), 3)
            self.assertEqual(origin.retained_versions(), (1, 2, 3))

            self.assertEqual(origin.publish(4, {"c": b"three"}), 4)
            self.assertEqual(origin.retained_versions(), (1, 3, 4))
            # Only version 1 is marked.  If restoring had copied its mark
            # onto 3, then 3 would also be mandatory here: four versions
            # with nothing eligible, which cannot fit max_versions=3.
            self.assertEqual(origin.publish(5, {"d": b"four"}), 5)
            self.assertEqual(origin.retained_versions(), (1, 4, 5))
            self.assertEqual(origin.active_version, 5)

    def test_restore_does_not_clear_the_source_mark(self):
        with ReleaseOrigin(max_bytes=4 * MIB, max_versions=2) as origin:
            origin.publish(1, {"a": b"one"})
            origin.publish(2, {"b": b"two"})
            origin.hold(2)
            self.assertEqual(origin.restore(2, 3), 3)
            self.assertEqual(origin.retained_versions(), (2, 3))

            # Mark 2 survived the restore: it is mandatory alongside current
            # 3 and new 4, which cannot fit two slots.  Had the restore
            # cleared the mark, 2 would be evictable and 4 would install.
            with self.assertRaises(ReleaseCapacityError):
                origin.publish(4, {"c": b"three"})
            self.assertEqual(origin.active_version, 3)
            self.assertEqual(origin.retained_versions(), (2, 3))
            self.assert_member(origin, member_target("b", b"two"), b"two")
            self.assert_missing(origin, member_target("c", b"three"))

            origin.unhold(2)
            self.assertEqual(origin.publish(4, {"c": b"three"}), 4)
            self.assertEqual(origin.retained_versions(), (3, 4))
            # Version 2 is gone, but 3 is its restored copy and still
            # references the very same member target.
            self.assert_member(origin, member_target("b", b"two"), b"two")
            self.assert_member(origin, member_target("c", b"three"), b"three")


class ManagementValidationTest(OriginTestCase):
    def test_constructor_validates_both_quotas(self):
        for value in (-1, 512 * MIB + 1, True, 1.5, "1024", None):
            with self.subTest(max_bytes=value):
                with self.assertRaises(ValueError):
                    ReleaseOrigin(max_bytes=value)
        for value in (0, 1, 9, -3, True, 2.5, "8", None):
            with self.subTest(max_versions=value):
                with self.assertRaises(ValueError):
                    ReleaseOrigin(max_bytes=KIB, max_versions=value)
        # Boundary values are accepted.
        ReleaseOrigin(max_bytes=0, max_versions=2).close()
        ReleaseOrigin(max_bytes=512 * MIB, max_versions=8).close()

    def test_zero_byte_budget_still_allows_empty_members(self):
        with ReleaseOrigin(max_bytes=0, max_versions=2) as origin:
            origin.publish(1, {"empty": b""})
            self.assertEqual(origin.active_version, 1)
            self.assert_member(origin, member_target("empty", b""), b"")
            with self.assertRaises(ReleaseCapacityError):
                origin.publish(2, {"nonempty": b"x"})
            self.assertEqual(origin.active_version, 1)
            self.assert_missing(origin, member_target("nonempty", b"x"))

    def test_publish_validates_versions_members_and_names(self):
        with ReleaseOrigin(max_bytes=8 * MIB) as origin:
            for name in ["", "-bad", ".bad", "_bad", "a" * 65, "bad name",
                         "bad/name", "bad#name", "bäd", 7, None, True]:
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        origin.publish(1, {name: b"body"})
            self.assertIsNone(origin.active_version)

            for members in (None, [], b"body", "text", {}):
                with self.subTest(members=members):
                    with self.assertRaises(ValueError):
                        origin.publish(1, members)
            with self.assertRaises(ValueError):
                origin.publish(
                    1, {f"m{index}": b"x" for index in range(65)})
            for body in (bytearray(b"x"), memoryview(b"x"), "text", 5, None):
                with self.subTest(body=body):
                    with self.assertRaises(ValueError):
                        origin.publish(1, {"a": body})
            with self.assertRaises(ValueError):
                origin.publish(1, {"a": b"x" * (MIB + 1)})
            for version in (0, -1, 1 << 63, True, 1.0, "1", None):
                with self.subTest(version=version):
                    with self.assertRaises(ValueError):
                        origin.publish(version, {"a": b"x"})
            self.assertIsNone(origin.active_version)
            self.assertEqual(origin.retained_versions(), ())

            # A 64-character logical name is the longest allowed form.
            origin.publish(5, {"a" * 64: b"ok"})
            self.assertEqual(origin.active_version, 5)

            for version in (5, 4, 1):
                with self.subTest(non_increasing=version):
                    with self.assertRaises(ValueError):
                        origin.publish(version, {"a": b"other"})
            self.assertEqual(origin.active_version, 5)
            self.assertEqual(origin.retained_versions(), (5,))
            # A rejected version number is not consumed.
            self.assertEqual(origin.publish(6, {"a": b"new"}), 6)

    def test_hold_unhold_restore_validate_arguments(self):
        with ReleaseOrigin(max_bytes=8 * MIB) as origin:
            origin.publish(1, {"a": b"one"})
            origin.publish(2, {"a": b"two"})
            for method in (origin.hold, origin.unhold):
                with self.subTest(method=method.__name__):
                    for version in (0, -1, 1 << 63, True, "1", None):
                        with self.assertRaises(ValueError):
                            method(version)
                    for version in (3, 99):
                        with self.assertRaises(KeyError):
                            method(version)
            self.assertEqual(origin.retained_versions(), (1, 2))
            self.assertEqual(origin.active_version, 2)

            for bad_source, bad_new in [
                (True, 3), (0, 3), (1 << 63, 3), ("1", 3), (None, 3),
                (1, True), (1, 0), (1, 1 << 63), (1, "3"), (1, None),
            ]:
                with self.subTest(source=bad_source, new=bad_new):
                    with self.assertRaises(ValueError):
                        origin.restore(bad_source, bad_new)
            for new in (2, 1):
                with self.subTest(non_increasing=new):
                    with self.assertRaises(ValueError):
                        origin.restore(1, new)
            with self.assertRaises(KeyError):
                origin.restore(7, 3)
            self.assertEqual(origin.retained_versions(), (1, 2))
            self.assertEqual(origin.active_version, 2)

            self.assertEqual(origin.restore(1, 3), 3)
            self.assertEqual(origin.active_version, 3)
            self.assertEqual(origin.retained_versions(), (1, 2, 3))
            self.assert_member(origin, member_target("a", b"one"), b"one")
            self.assert_member(origin, member_target("a", b"two"), b"two")

    def test_retired_versions_raise_key_error_and_empty_origin_restore(self):
        with ReleaseOrigin(max_bytes=8 * MIB) as origin:
            with self.assertRaises(KeyError):
                origin.restore(1, 2)
            with self.assertRaises(KeyError):
                origin.hold(1)
            self.assertIsNone(origin.active_version)

        with ReleaseOrigin(max_bytes=8 * MIB, max_versions=2) as origin:
            origin.publish(1, {"a": b"one"})
            origin.publish(2, {"a": b"two"})
            origin.publish(3, {"a": b"three"})
            self.assertEqual(origin.retained_versions(), (2, 3))
            for method in (
                lambda: origin.hold(1),
                lambda: origin.unhold(1),
                lambda: origin.restore(1, 4),
            ):
                with self.assertRaises(KeyError):
                    method()
            self.assertEqual(origin.retained_versions(), (2, 3))
            self.assertEqual(origin.active_version, 3)
            # Validation of a new version that is not increasing still wins
            # over capacity or lookup errors.
            with self.assertRaises(ValueError):
                origin.restore(3, 3)


if __name__ == "__main__":
    unittest.main()
