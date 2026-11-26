"""Standard-library tests for whole-release ReleaseReader."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from reader import OriginError, ReleaseReader, ReleaseUnavailable


PROJECT = Path(__file__).resolve().parent
MIB = 1024 * 1024
MANIFEST = "/release/manifest.json"


def sha256_bytes(body):
    return hashlib.sha256(body).hexdigest()


def member_record(target, body):
    return {"target": target, "size": len(body), "sha256": sha256_bytes(body)}


def make_manifest(version, serve_min, members):
    return {"version": version, "serve_min": serve_min, "members": members}


def json_bytes(value):
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def ok_response(body, headers=(), content_length=None):
    header_list = list(headers)
    if content_length is None:
        length = body.size if hasattr(body, "size") else len(body)
    else:
        length = content_length
    header_list.append(("Content-Length", str(length)))
    return 200, header_list, body


def logical_bytes(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except FileNotFoundError:
                # A concurrent boundary sampler may race rename/unlink.
                pass
    return total


class PatternBody:
    """Deterministic large origin body which does not reside in test memory."""

    def __init__(self, seed, size=MIB):
        self.seed = seed
        self.size = size
        self.pattern = bytes((seed + index) % 256 for index in range(64 * 1024))

    def _blocks(self):
        repeats, remainder = divmod(self.size, len(self.pattern))
        for _ in range(repeats):
            yield self.pattern
        if remainder:
            yield self.pattern[:remainder]

    def write_to(self, wfile):
        for block in self._blocks():
            wfile.write(block)

    def digest(self):
        digest = hashlib.sha256()
        for block in self._blocks():
            digest.update(block)
        return digest.hexdigest()


class ScriptedReleaseOrigin:
    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.requests = []
        self.lock = threading.Lock()
        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self):
                with origin.lock:
                    origin.requests.append(
                        (self.path, self.headers.get("If-None-Match")))

                route = origin.routes.get(self.path)
                spec = route(self.path) if callable(route) else route
                if spec is None:
                    body = b"not found"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                status, headers, body = spec
                try:
                    self.send_response(status)
                    for name, value in headers:
                        self.send_header(name, value)
                    self.end_headers()
                    if getattr(body, "write_to", None) is not None:
                        body.write_to(self.wfile)
                    elif body:
                        self.wfile.write(body)
                except OSError:
                    # Deliberately broken/truncated cases can close first.
                    pass

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        return False

    @property
    def paths(self):
        with self.lock:
            return [path for path, _validator in self.requests]

    def clear(self):
        with self.lock:
            self.requests.clear()

    def member_paths(self, manifest_target=MANIFEST):
        return [path for path in self.paths if path != manifest_target]

    def publish(self, value, *, raw=None, headers=()):
        body = raw if raw is not None else json_bytes(value)
        all_headers = [("Content-Length", str(len(body))), *headers]
        self.routes[MANIFEST] = (200, all_headers, body)


class ReleaseReaderLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.mkdtemp(prefix="release-reader-test-")

    def tearDown(self):
        shutil.rmtree(self.scratch, ignore_errors=True)

    def make_reader(self, port=0):
        return ReleaseReader(
            "127.0.0.1", port, MANIFEST, scratch_dir=self.scratch, timeout=5)

    def private_root(self, reader=None):
        if reader is not None:
            return reader._root
        roots = [
            os.path.join(self.scratch, name)
            for name in os.listdir(self.scratch)
            if name.startswith("release-reader-")
        ]
        self.assertEqual(len(roots), 1)
        return roots[0]

    def test_constructor_validates_manifest_target_and_close_is_idempotent(self):
        for bad in [
            "relative", "http://127.0.0.1/x", "//host/x", "/x#f",
            "/x y", "/\xff", "/" + "a" * 256,
        ]:
            with self.assertRaises(ValueError):
                ReleaseReader(
                    "127.0.0.1", 0, bad, scratch_dir=self.scratch)

        reader = self.make_reader()
        root = reader._root
        caller_file = os.path.join(self.scratch, "caller-owned")
        with open(caller_file, "w", encoding="utf-8") as handle:
            handle.write("keep me")
        self.assertTrue(os.path.isdir(root))
        self.assertIsNone(reader.active_version)
        with self.assertRaises(ReleaseUnavailable):
            reader.read_active("/anything")

        reader.close()
        reader.close()
        self.assertFalse(os.path.exists(root))
        self.assertTrue(os.path.isfile(caller_file))
        self.assertEqual(
            sorted(os.listdir(self.scratch)), ["caller-owned"])
        with self.assertRaises(RuntimeError):
            reader.refresh()
        with self.assertRaises(RuntimeError):
            reader.read_active("/anything")

    def test_required_reuse_retirement_rejection_and_reinstall_sequence(self):
        unchanged_target = "/cfg/unchanged?generation=11"
        changing_target = "/cfg/changing"
        removed_target = "/cfg/removed"

        unchanged_body = b"unchanged-v10-" * 11  # 154 bytes
        removed_body = b"removed-old-release" * 13  # 247 bytes
        changing_v10 = b"changing-v10-" * 250  # 3250 bytes
        changing_v12 = b"changing-v12-new-" * 400  # 6800 bytes
        bad_changing = b"Z" * len(changing_v12)
        self.assertNotEqual(
            sha256_bytes(changing_v12), sha256_bytes(bad_changing))

        v10 = make_manifest(10, 10, [
            member_record(unchanged_target, unchanged_body),
            member_record(changing_target, changing_v10),
            member_record(removed_target, removed_body),
        ])
        v11 = make_manifest(11, 10, [
            member_record(unchanged_target, unchanged_body),
            member_record(changing_target, changing_v12),
        ])
        v12 = make_manifest(12, 12, v11["members"])

        routes = {
            unchanged_target: ok_response(unchanged_body),
            changing_target: ok_response(changing_v10),
            removed_target: ok_response(removed_body),
        }
        with ScriptedReleaseOrigin(routes) as origin:
            origin.publish(v10)
            reader = self.make_reader(origin.port)

            self.assertEqual(reader.refresh(), 10)
            self.assertEqual(reader.active_version, 10)
            self.assertEqual(
                reader.read_active(unchanged_target), unchanged_body)
            self.assertEqual(
                reader.read_active(changing_target), changing_v10)
            self.assertEqual(reader.read_active(removed_target), removed_body)
            origin.clear()

            # Version 11 keeps serving floor at 10, so failure retains v10.
            origin.publish(v11)
            origin.routes[unchanged_target] = (
                503, [("Content-Length", "0")], b"")
            origin.routes[changing_target] = ok_response(bad_changing)

            with self.assertRaises(OriginError):
                reader.refresh()

            self.assertEqual(reader.active_version, 10)
            self.assertEqual(
                reader.read_active(unchanged_target), unchanged_body)
            self.assertEqual(
                reader.read_active(changing_target), changing_v10)
            self.assertEqual(reader.read_active(removed_target), removed_body)
            self.assertEqual(origin.member_paths(), [changing_target])
            origin.clear()

            # Floor 12 retires active v10 even though v12 installation fails.
            origin.publish(v12)
            with self.assertRaises(OriginError):
                reader.refresh()

            self.assertIsNone(reader.active_version)
            with self.assertRaises(ReleaseUnavailable):
                reader.read_active(unchanged_target)
            with self.assertRaises(ReleaseUnavailable):
                reader.read_active(changing_target)
            with self.assertRaises(ReleaseUnavailable):
                reader.read_active(removed_target)
            self.assertEqual(origin.member_paths(), [changing_target])
            root = self.private_root(reader)
            self.assertEqual(logical_bytes(root), 0)
            origin.clear()

            # Backward version: structurally valid version 11/floor 11.
            backward = make_manifest(11, 11, v12["members"])
            origin.publish(backward)
            with self.assertRaises(OriginError):
                reader.refresh()
            self.assertEqual(origin.member_paths(), [])
            origin.clear()

            # Same version with altered member content is not a retry.
            altered = json.loads(json_bytes(v12))
            altered["members"][1]["sha256"] = "0" * 64
            origin.publish(altered)
            with self.assertRaises(OriginError):
                reader.refresh()
            self.assertEqual(origin.member_paths(), [])
            origin.clear()

            # A newer version cannot lower the accepted service floor.
            lower_floor = make_manifest(13, 11, v12["members"])
            origin.publish(lower_floor)
            with self.assertRaises(OriginError):
                reader.refresh()
            self.assertEqual(origin.member_paths(), [])
            origin.clear()

            # Restore the origin and retry the original accepted v12.  There
            # is no active release at this refresh start, so no byte reuse.
            origin.routes[unchanged_target] = ok_response(unchanged_body)
            origin.routes[changing_target] = ok_response(changing_v12)
            origin.publish(v12)

            self.assertEqual(reader.refresh(), 12)
            self.assertEqual(reader.active_version, 12)
            self.assertEqual(
                reader.read_active(unchanged_target), unchanged_body)
            self.assertEqual(
                reader.read_active(changing_target), changing_v12)
            with self.assertRaises(KeyError):
                reader.read_active(removed_target)
            self.assertCountEqual(
                origin.member_paths(),
                [unchanged_target, changing_target])
            self.assertEqual(
                logical_bytes(reader._root),
                len(unchanged_body) + len(changing_v12))
            origin.clear()

            # The installed same manifest only re-reads the manifest.
            self.assertEqual(reader.refresh(), 12)
            self.assertEqual(origin.member_paths(), [])
            self.assertEqual(origin.paths, [MANIFEST])

            reader.close()
            self.assertFalse(os.path.exists(reader._root))
            self.assertEqual(os.listdir(self.scratch), [])

    def test_invalid_manifests_are_rejected_before_member_fetch(self):
        body = b"member bytes"
        target = "/valid-member"
        valid = make_manifest(1, 0, [member_record(target, body)])

        invalid_values = [
            {},
            {"version": 1, "serve_min": 0},
            {"version": 1, "serve_min": 0, "members": [], "extra": 1},
            make_manifest(True, 0, [member_record(target, body)]),
            make_manifest(0, 0, [member_record(target, body)]),
            make_manifest(1 << 63, 0, [member_record(target, body)]),
            make_manifest(1, True, [member_record(target, body)]),
            make_manifest(1, -1, [member_record(target, body)]),
            make_manifest(1, 2, [member_record(target, body)]),
            make_manifest(1, 0, "not-a-list"),
            make_manifest(1, 0, []),
            make_manifest(1, 0, [
                member_record(f"/m{i}", b"x") for i in range(65)]),
            make_manifest(1, 0, [{
                "target": target, "size": len(body),
                "sha256": sha256_bytes(body), "extra": False}]),
            make_manifest(1, 0, [{
                "target": "http://absolute/x", "size": len(body),
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": "//host/x", "size": len(body),
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": "/x#fragment", "size": len(body),
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": "/" + "a" * 256, "size": len(body),
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": MANIFEST, "size": len(body),
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [
                member_record("/dup", b"a"), member_record("/dup", b"b")]),
            make_manifest(1, 0, [{
                "target": target, "size": True,
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": target, "size": -1,
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": target, "size": MIB + 1,
                "sha256": sha256_bytes(body)}]),
            make_manifest(1, 0, [{
                "target": target, "size": len(body),
                "sha256": sha256_bytes(body).upper()}]),
            make_manifest(1, 0, [{
                "target": target, "size": len(body), "sha256": "abc"}]),
        ]

        invalid_raw = [
            b'{"version":1,"version":2,"serve_min":0,"members":[]}',
            b'{"version":1,"serve_min":0,\xff"members":[]}',
            b"{not json",
            b"   ",
            b"null",
            json_bytes(valid) + b" " * (32 * 1024 + 1),
        ]

        with ScriptedReleaseOrigin({target: ok_response(body)}) as origin:
            reader = self.make_reader(origin.port)
            for index, value in enumerate(invalid_values):
                with self.subTest(kind="value", index=index):
                    origin.publish(value)
                    origin.clear()
                    with self.assertRaises(OriginError):
                        reader.refresh()
                    self.assertIsNone(reader.active_version)
                    self.assertEqual(origin.member_paths(), [])

            for index, raw in enumerate(invalid_raw):
                with self.subTest(kind="raw", index=index):
                    origin.publish(None, raw=raw)
                    origin.clear()
                    with self.assertRaises(OriginError):
                        reader.refresh()
                    self.assertIsNone(reader.active_version)
                    self.assertEqual(origin.member_paths(), [])

            # A rejected first manifest does not establish rollback baseline.
            origin.publish(valid)
            self.assertEqual(reader.refresh(), 1)
            self.assertEqual(reader.read_active(target), body)
            reader.close()

    def test_truncated_manifest_does_not_establish_baseline(self):
        body = b"manifest-after-truncation"
        target = "/after-truncated-manifest"
        valid = make_manifest(1, 0, [member_record(target, body)])
        raw = json_bytes(valid)
        truncated = (
            200,
            [("Content-Length", str(len(raw) + 64))],
            raw,
        )
        with ScriptedReleaseOrigin({target: ok_response(body)}) as origin:
            origin.routes[MANIFEST] = truncated
            reader = self.make_reader(origin.port)
            with self.assertRaises(OriginError):
                reader.refresh()
            self.assertIsNone(reader.active_version)
            self.assertEqual(origin.member_paths(), [])

            origin.publish(valid)
            self.assertEqual(reader.refresh(), 1)
            self.assertEqual(reader.read_active(target), body)
            reader.close()

    def test_truncated_member_installs_nothing_and_leaves_no_candidate(self):
        expected = b"abcdefghij" * 10  # 100 bytes
        target = "/truncated"
        manifest = make_manifest(
            3, 0, [member_record(target, expected)])
        truncated_spec = (
            200, [("Content-Length", str(len(expected)))], b"short")

        with ScriptedReleaseOrigin({target: truncated_spec}) as origin:
            origin.publish(manifest)
            reader = self.make_reader(origin.port)

            for _ in range(2):
                with self.assertRaises(OriginError):
                    reader.refresh()
                self.assertIsNone(reader.active_version)
                with self.assertRaises(ReleaseUnavailable):
                    reader.read_active(target)
                self.assertEqual(logical_bytes(reader._root), 0)

            # Exactly one private instance root and no accumulated stage.
            self.assertEqual(len(os.listdir(self.scratch)), 1)
            reader.close()

    def test_non_installable_and_unsupported_encoding_responses_fail(self):
        body = b"do not retain"
        target = "/header-policy"
        manifest = make_manifest(
            4, 0, [member_record(target, body)])

        header_cases = [
            [("Cache-Control", "no-store")],
            [("Vary", "*")],
            [("Set-Cookie", "session=secret")],
            [("Content-Encoding", "gzip")],
        ]
        with ScriptedReleaseOrigin() as origin:
            origin.publish(manifest)
            reader = self.make_reader(origin.port)
            for headers in header_cases:
                origin.routes[target] = ok_response(body, headers=headers)
                with self.assertRaises(OriginError):
                    reader.refresh()
                self.assertIsNone(reader.active_version)
                self.assertEqual(logical_bytes(reader._root), 0)

            origin.routes[target] = (
                200,
                [("Transfer-Encoding", "chunked")],
                b"4\r\nWiki\r\n0\r\n\r\n",
            )
            with self.assertRaises(OriginError):
                reader.refresh()
            self.assertIsNone(reader.active_version)
            reader.close()

    def test_manifest_retention_header_does_not_establish_baseline(self):
        body = b"after no-store manifest"
        target = "/ordinary"
        valid = make_manifest(5, 0, [member_record(target, body)])
        raw = json_bytes(valid)
        no_store_manifest = (
            200,
            [
                ("Content-Length", str(len(raw))),
                ("Cache-Control", "no-store"),
            ],
            raw,
        )
        with ScriptedReleaseOrigin({target: ok_response(body)}) as origin:
            origin.routes[MANIFEST] = no_store_manifest
            reader = self.make_reader(origin.port)
            with self.assertRaises(OriginError):
                reader.refresh()

            # The transport-rejected manifest was not accepted as baseline;
            # serving the same version without the header can now install.
            origin.publish(valid)
            self.assertEqual(reader.refresh(), 5)
            self.assertEqual(reader.read_active(target), body)
            reader.close()

    def test_same_manifest_allows_whitespace_key_order_and_member_order(self):
        body_a = b"alpha"
        body_b = b"beta-longer"
        target_a = "/alpha"
        target_b = "/beta"
        manifest = make_manifest(2, 0, [
            member_record(target_a, body_a),
            member_record(target_b, body_b),
        ])
        reordered_raw = json.dumps({
            "members": [
                {
                    "sha256": sha256_bytes(body_b),
                    "target": target_b,
                    "size": len(body_b),
                },
                {
                    "size": len(body_a),
                    "sha256": sha256_bytes(body_a),
                    "target": target_a,
                },
            ],
            "serve_min": 0,
            "version": 2,
        }, indent=7).encode("utf-8")

        routes = {
            target_a: ok_response(body_a),
            target_b: ok_response(body_b),
        }
        with ScriptedReleaseOrigin(routes) as origin:
            origin.publish(manifest)
            reader = self.make_reader(origin.port)
            self.assertEqual(reader.refresh(), 2)

            origin.routes[MANIFEST] = (
                200,
                [("Content-Length", str(len(reordered_raw)))],
                reordered_raw,
            )
            origin.clear()
            self.assertEqual(reader.refresh(), 2)
            self.assertEqual(origin.member_paths(), [])
            self.assertEqual(reader.read_active(target_a), body_a)
            self.assertEqual(reader.read_active(target_b), body_b)
            reader.close()

    def test_key_error_for_missing_target_but_release_remains_active(self):
        body = b"present"
        target = "/present"
        manifest = make_manifest(
            8, 0, [member_record(target, body)])
        with ScriptedReleaseOrigin({target: ok_response(body)}) as origin:
            origin.publish(manifest)
            reader = self.make_reader(origin.port)
            reader.refresh()
            with self.assertRaises(KeyError):
                reader.read_active("/missing")
            self.assertEqual(reader.active_version, 8)
            self.assertEqual(reader.read_active(target), body)
            reader.close()


MEMORY_CLIENT = r"""
import json
import sys
import tracemalloc

project, host, port, scratch, manifest_target = sys.argv[1:6]
sys.path.insert(0, project)
tracemalloc.start()

from reader import ReleaseReader  # noqa: E402

reader = ReleaseReader(
    host, int(port), manifest_target, scratch_dir=scratch, timeout=30)
version1 = reader.refresh()
first = reader.read_active("/big/a/00")
first_length = len(first)
first_sha = __import__("hashlib").sha256(first).hexdigest()
del first

version2 = reader.refresh()
second = reader.read_active("/big/b/00")
second_length = len(second)
second_sha = __import__("hashlib").sha256(second).hexdigest()

_current, peak = tracemalloc.get_traced_memory()


def logical_bytes(path):
    total = 0
    for root, _dirs, files in __import__("os").walk(path):
        for name in files:
            total += __import__("os").path.getsize(
                __import__("os").path.join(root, name))
    return total


result = {
    "version1": version1,
    "version2": version2,
    "first_length": first_length,
    "first_sha": first_sha,
    "second_length": second_length,
    "second_sha": second_sha,
    "peak": peak,
    "workspace": logical_bytes(reader._root),
    "root": reader._root,
}
print("MEMRESULT=" + json.dumps(result))
reader.close()
"""


class MemoryAndWorkspaceBoundaryTest(unittest.TestCase):
    def test_two_full_sized_releases_stay_under_memory_and_disk_budgets(self):
        scratch = tempfile.mkdtemp(prefix="release-memory-test-")
        self.addCleanup(shutil.rmtree, scratch, ignore_errors=True)

        count = 64
        a_targets = [f"/big/a/{index:02d}" for index in range(count)]
        b_targets = [f"/big/b/{index:02d}" for index in range(count)]
        a_bodies = [PatternBody(17 + index) for index in range(count)]
        b_bodies = [PatternBody(129 + index) for index in range(count)]

        a_manifest = make_manifest(101, 0, [
            {"target": target, "size": MIB, "sha256": body.digest()}
            for target, body in zip(a_targets, a_bodies)
        ])
        b_manifest = make_manifest(102, 101, [
            {"target": target, "size": MIB, "sha256": body.digest()}
            for target, body in zip(b_targets, b_bodies)
        ])

        routes = {}
        for target, body in zip(a_targets, a_bodies):
            routes[target] = ok_response(body)
        for target, body in zip(b_targets, b_bodies):
            routes[target] = ok_response(body)

        manifest_calls = 0

        def manifest_route(_path):
            nonlocal manifest_calls
            manifest_calls += 1
            value = a_manifest if manifest_calls == 1 else b_manifest
            data = json_bytes(value)
            return ok_response(data)

        routes[MANIFEST] = manifest_route

        with ScriptedReleaseOrigin(routes) as origin:
            process = subprocess.Popen(
                [
                    sys.executable, "-I", "-c", MEMORY_CLIENT,
                    str(PROJECT), "127.0.0.1", str(origin.port),
                    scratch, MANIFEST,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            peak_workspace = 0
            try:
                while process.poll() is None:
                    peak_workspace = max(
                        peak_workspace, logical_bytes(scratch))
                    time.sleep(0.005)
                stdout, stderr = process.communicate(timeout=120)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

        self.assertEqual(
            process.returncode, 0, msg=f"stdout={stdout}\nstderr={stderr}")
        # Two disjoint 64-member releases have no reusable target/spec.
        self.assertEqual(len(origin.member_paths()), 128)
        self.assertEqual(origin.paths.count(MANIFEST), 2)

        lines = [line for line in stdout.splitlines()
                 if line.startswith("MEMRESULT=")]
        self.assertEqual(len(lines), 1, msg=stdout + stderr)
        result = json.loads(lines[0].split("=", 1)[1])

        self.assertEqual(result["version1"], 101)
        self.assertEqual(result["version2"], 102)
        self.assertEqual(result["first_length"], MIB)
        self.assertEqual(result["second_length"], MIB)
        self.assertEqual(result["first_sha"], a_bodies[0].digest())
        self.assertEqual(result["second_sha"], b_bodies[0].digest())

        # 8 MiB tracemalloc budget and 132 MiB logical workspace budget.
        self.assertLessEqual(result["peak"], 8 * MIB, msg=result)
        self.assertEqual(result["workspace"], 64 * MIB)
        self.assertLessEqual(peak_workspace, 132 * MIB)

        # The client closed its private workspace; no caller files existed.
        self.assertFalse(os.path.exists(result["root"]))
        self.assertEqual(os.listdir(scratch), [])


if __name__ == "__main__":
    unittest.main()
