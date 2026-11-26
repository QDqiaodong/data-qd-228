"""Loopback tests for MirrorReader primary/standby failover."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
import unittest

from reader import MirrorReader, OriginError, Representation


def conditional(body, etag):
    """Route spec: 304 when If-None-Match matches, else a retainable 200."""
    return ("cond", body, etag)


def fixed(status, etag=None):
    """Route spec: an unconditional status with optional ETag and no body."""
    return ("fixed", status, etag)


def truncated(declared_length, partial_body):
    """Route spec: 200 with a legal Content-Length but a shorter real body."""
    return ("truncate", declared_length, partial_body)


DROP = ("drop",)


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        origin = self.server.origin
        validator = self.headers.get("If-None-Match")
        with origin.lock:
            origin.log.append((self.path, validator))
        spec = origin.routes.get(self.path, fixed(404))
        kind = spec[0]

        if kind == "drop":
            # Close without any response, like an unresponsive origin.
            self.close_connection = True
            return
        if kind == "cond":
            _, body, etag = spec
            if etag is not None and validator == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            if etag is not None:
                self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)
            return
        if kind == "fixed":
            _, status, etag = spec
            self.send_response(status)
            if etag is not None:
                self.send_header("ETag", etag)
            self.end_headers()
            return
        if kind == "truncate":
            _, declared_length, partial_body = spec
            self.send_response(200)
            self.send_header("Content-Length", str(declared_length))
            self.end_headers()
            self.wfile.write(partial_body)
            return
        raise AssertionError(f"unknown route spec: {spec!r}")

    def log_message(self, *_):
        pass


class _OriginServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # dropped/truncated test routes must not print tracebacks


class Origin:
    """One scripted loopback origin recording (path, validator) per GET."""

    def __init__(self, routes):
        self.routes = dict(routes)
        self.log = []
        self.lock = threading.Lock()
        self.server = _OriginServer(("127.0.0.1", 0), _Handler)
        self.server.origin = self
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01}, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        return False

    @property
    def seen(self):
        with self.lock:
            return list(self.log)

    def paths(self):
        return [path for path, _ in self.seen]


class FakeClock:
    def __init__(self, value=0.0):
        self.value = float(value)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


def unused_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class MirrorValidationTest(unittest.TestCase):
    def test_constructor_rejects_bad_arguments(self):
        primary = ("127.0.0.1", 8001)
        standby = ("127.0.0.1", 8002)
        with self.assertRaises(ValueError):
            MirrorReader(primary, primary)
        for bad in [None, 8001, "", (), ("", 8001), (None, 8001), (8001, 8001),
                    ("h",), ("h", 0), ("h", 65536), ("h", True), ("h", 1.5),
                    ("h", "80"), ("h", 80, 1)]:
            with self.assertRaises(ValueError, msg=repr(bad)):
                MirrorReader(bad, standby)
            with self.assertRaises(ValueError, msg=repr(bad)):
                MirrorReader(primary, bad)
        for kwargs in [
                {"entries": 0}, {"entries": True}, {"entries": 1.5},
                {"max_body": 0}, {"max_body": False}, {"max_body": 1.5},
                {"failures": 0}, {"failures": 9}, {"failures": True},
                {"failures": 2.0},
                {"cooldown": 0}, {"cooldown": -1}, {"cooldown": True},
                {"cooldown": float("nan")}, {"cooldown": float("inf")},
                {"cooldown": "10"},
                {"timeout": 0}, {"timeout": -1}, {"timeout": True},
                {"timeout": float("nan")}, {"timeout": float("inf")}]:
            with self.assertRaises(ValueError, msg=repr(kwargs)):
                MirrorReader(primary, standby, **kwargs)

    def test_constructor_does_not_use_network(self):
        # Reserved unroutable addresses (RFC 5737); construction must not
        # open any connection.
        MirrorReader(("192.0.2.1", 8080), ("192.0.2.2", 8080))

    def test_invalid_target_sends_no_requests(self):
        with Origin({}) as primary, Origin({}) as standby:
            reader = MirrorReader(("127.0.0.1", primary.port),
                                  ("127.0.0.1", standby.port))
            for bad in ["", "relative", "//x/a", "http://x/a", "/a#f", "/a b",
                        None, 5]:
                with self.assertRaises(ValueError, msg=repr(bad)):
                    reader.get(bad)
            self.assertEqual(primary.seen, [])
            self.assertEqual(standby.seen, [])


class MirrorFailoverTest(unittest.TestCase):
    def make_reader(self, primary, standby, clock, **kwargs):
        return MirrorReader(
            ("127.0.0.1", primary.port), ("127.0.0.1", standby.port),
            clock=clock, **kwargs)

    def test_same_strong_etag_stays_per_source(self):
        clock = FakeClock(100)
        primary_routes = {
            "/res": conditional(b"primary-bytes", '"same"'),
            "/x": fixed(503),
            "/y": fixed(503),
        }
        standby_routes = {
            "/res": conditional(b"standby-bytes", '"same"'),
            "/x": conditional(b"standby-x", '"sx"'),
            "/y": conditional(b"standby-y", '"sy"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10)

            self.assertEqual(reader.get("/res"),
                             Representation(b"primary-bytes", False))
            self.assertEqual(reader.get("/x"), Representation(b"standby-x", False))
            self.assertEqual(reader.get("/y"), Representation(b"standby-y", False))
            self.assertEqual(primary.seen,
                             [("/res", None), ("/x", None), ("/y", None)])

            # Cooling down: only the standby is asked.  Its first GET for the
            # target must not carry the primary's identical ETag, and its own
            # 304 redeems standby bytes.
            clock.advance(5)
            self.assertEqual(reader.get("/res"),
                             Representation(b"standby-bytes", False))
            self.assertEqual(reader.get("/res"),
                             Representation(b"standby-bytes", True))
            self.assertEqual(standby.seen[-2:],
                             [("/res", None), ("/res", '"same"')])
            self.assertEqual(len(primary.seen), 3)

            # At the exact deadline the call probes the primary with the real
            # target; the primary's matching 304 delivers primary bytes even
            # though the standby may also answer.
            standby_before_probe = len(standby.seen)
            clock.advance(5)
            self.assertEqual(reader.get("/res"),
                             Representation(b"primary-bytes", True))
            self.assertEqual(primary.seen[-1], ("/res", '"same"'))
            self.assertLessEqual(
                len(standby.seen) - standby_before_probe, 1,
                "a probe may add at most one standby GET")

            # The successful probe returns reads to the primary-only path.
            standby_after_probe = len(standby.seen)
            self.assertEqual(reader.get("/res"),
                             Representation(b"primary-bytes", True))
            self.assertEqual(primary.seen[-1], ("/res", '"same"'))
            self.assertEqual(len(standby.seen), standby_after_probe)

    def test_cooldown_probe_renewal_and_authoritative_exit(self):
        clock = FakeClock(0)
        primary_routes = {
            "/a": fixed(503),
            "/b": fixed(503),
            "/d": fixed(503),
            "/f": fixed(410),
            "/g": conditional(b"primary-g", '"pg"'),
        }
        standby_routes = {
            "/a": conditional(b"standby-a", '"sa"'),
            "/b": conditional(b"standby-b", '"sb"'),
            "/c": fixed(500),
            "/d": conditional(b"standby-d", '"sd"'),
            "/e": conditional(b"standby-e", '"se"'),
            "/f": conditional(b"standby-f", '"sf"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10)
            self.assertEqual(reader.get("/a"), Representation(b"standby-a", False))
            self.assertEqual(reader.get("/b"), Representation(b"standby-b", False))
            self.assertEqual(primary.paths(), ["/a", "/b"])

            # Inside the cooldown a standby error is reported as-is and the
            # primary sees no extra request.
            with self.assertRaises(OriginError):
                reader.get("/c")
            self.assertEqual(primary.paths(), ["/a", "/b"])
            self.assertEqual(standby.paths(), ["/a", "/b", "/c"])

            # At the deadline the probe meets primary 503: a new full cooldown
            # starts immediately and this call delivers the standby bytes.
            clock.advance(10)
            self.assertEqual(reader.get("/d"), Representation(b"standby-d", False))
            self.assertEqual(primary.paths().count("/d"), 1)
            self.assertEqual(standby.paths().count("/d"), 1)

            # The old cooldown has expired but the new one has not: the next
            # read must not touch the primary at all.
            self.assertEqual(reader.get("/e"), Representation(b"standby-e", False))
            self.assertNotIn("/e", primary.paths())

            # At the new deadline the probe meets an authoritative 410: the
            # primary error is reported and no standby bytes are delivered.
            clock.advance(10)
            standby_before_probe = len(standby.seen)
            with self.assertRaisesRegex(OriginError, "410"):
                reader.get("/f")
            self.assertEqual(primary.paths().count("/f"), 1)
            self.assertLessEqual(len(standby.seen) - standby_before_probe, 1)

            # The next normal read is primary-first again.
            self.assertEqual(reader.get("/g"), Representation(b"primary-g", False))
            self.assertEqual(primary.paths()[-1], "/g")

    def test_normal_path_diversion_classification(self):
        clock = FakeClock(0)
        primary_routes = {
            "/trunc": truncated(10, b"1234"),
            "/gone": fixed(404),
            "/r": conditional(b"r-primary", '"r"'),
            "/big": conditional(b"x" * 100, '"big"'),
        }
        standby_routes = {
            "/trunc": conditional(b"standby-trunc", '"st"'),
            "/gone": conditional(b"standby-gone", '"sg"'),
            "/r": conditional(b"r-standby", '"rs"'),
            "/big": conditional(b"standby-big", '"sb"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10, max_body=64)

            # A truncated primary body after a legal length header diverts.
            self.assertEqual(reader.get("/trunc"),
                             Representation(b"standby-trunc", False))
            self.assertEqual(standby.seen, [("/trunc", None)])

            # An authoritative primary 404 is reported; the standby is not asked.
            with self.assertRaisesRegex(OriginError, "404"):
                reader.get("/gone")
            self.assertNotIn("/gone", standby.paths())

            # A 304 that does not confirm the sent validator is reported.
            self.assertEqual(reader.get("/r"), Representation(b"r-primary", False))
            primary.routes["/r"] = fixed(304, '"other"')
            with self.assertRaises(OriginError):
                reader.get("/r")
            self.assertNotIn("/r", standby.paths())

            # A 200 beyond max_body is a protocol failure, not divertable.
            with self.assertRaises(OriginError):
                reader.get("/big")
            self.assertNotIn("/big", standby.paths())

    def test_failure_streak_resets_on_primary_success(self):
        clock = FakeClock(0)
        primary_routes = {
            "/f1": fixed(503),
            "/ok": conditional(b"primary-ok", '"po"'),
            "/f2": fixed(503),
            "/after": conditional(b"primary-after", '"pa"'),
        }
        standby_routes = {
            "/f1": conditional(b"standby-f1", '"s1"'),
            "/f2": conditional(b"standby-f2", '"s2"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10)
            self.assertEqual(reader.get("/f1"), Representation(b"standby-f1", False))
            self.assertEqual(reader.get("/ok"), Representation(b"primary-ok", False))
            self.assertEqual(reader.get("/f2"), Representation(b"standby-f2", False))
            # The streak is back to one: still the normal path, primary first.
            self.assertEqual(reader.get("/after"),
                             Representation(b"primary-after", False))
            self.assertEqual(primary.paths(), ["/f1", "/ok", "/f2", "/after"])
            self.assertNotIn("/after", standby.paths())

    def test_invalid_target_does_not_trigger_probe(self):
        clock = FakeClock(0)
        primary_routes = {
            "/f1": fixed(503),
            "/f2": fixed(503),
            "/ok": conditional(b"primary-ok", '"po"'),
        }
        standby_routes = {
            "/f1": conditional(b"standby-f1", '"s1"'),
            "/f2": conditional(b"standby-f2", '"s2"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10)
            reader.get("/f1")
            reader.get("/f2")
            clock.advance(10)  # the cooldown deadline is reached
            primary_calls = len(primary.seen)
            standby_calls = len(standby.seen)
            for bad in ["", "relative", "//x", "/a#f", "/a b", None]:
                with self.assertRaises(ValueError, msg=repr(bad)):
                    reader.get(bad)
            self.assertEqual(len(primary.seen), primary_calls)
            self.assertEqual(len(standby.seen), standby_calls)
            # The expired cooldown still probes on the next legal call.
            self.assertEqual(reader.get("/ok"),
                             Representation(b"primary-ok", False))
            self.assertEqual(primary.seen[-1], ("/ok", None))

    def test_transport_errors_are_divertable(self):
        clock = FakeClock(0)
        with Origin({"/drop": DROP}) as primary, Origin({
                "/drop": conditional(b"standby-drop", '"sd"'),
                "/refused": conditional(b"standby-refused", '"sr"')}) as standby:
            reader = self.make_reader(primary, standby, clock)
            # The primary disconnects without any response.
            self.assertEqual(reader.get("/drop"),
                             Representation(b"standby-drop", False))
            self.assertEqual(standby.seen, [("/drop", None)])

            # A refused connection diverts the same way.
            dead = MirrorReader(("127.0.0.1", unused_port()),
                                ("127.0.0.1", standby.port), clock=clock)
            self.assertEqual(dead.get("/refused"),
                             Representation(b"standby-refused", False))

    def test_gone_status_removes_only_the_answering_sources_entry(self):
        clock = FakeClock(0)
        primary_routes = {
            "/z": conditional(b"primary-z", '"pz"'),
            "/f1": fixed(503),
            "/f2": fixed(503),
        }
        standby_routes = {
            "/z": conditional(b"standby-z", '"sz"'),
            "/f1": conditional(b"standby-f1", '"s1"'),
            "/f2": conditional(b"standby-f2", '"s2"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      failures=2, cooldown=10)
            self.assertEqual(reader.get("/z"), Representation(b"primary-z", False))

            # A divertable failure warms the standby cache for the same target.
            primary.routes["/z"] = fixed(503)
            self.assertEqual(reader.get("/z"), Representation(b"standby-z", False))

            # An authoritative 410 from the primary removes only its own entry.
            primary.routes["/z"] = fixed(410)
            with self.assertRaisesRegex(OriginError, "410"):
                reader.get("/z")

            # Enter cooldown; the standby still has its own confirmed entry.
            reader.get("/f1")
            reader.get("/f2")
            self.assertEqual(reader.get("/z"), Representation(b"standby-z", True))
            self.assertEqual(standby.seen[-1], ("/z", '"sz"'))

            # After cooldown expiry the primary is probed with no retained
            # validator, because its own entry was removed by the 410.
            primary.routes["/z"] = conditional(b"primary-z2", '"pz2"')
            clock.advance(10)
            self.assertEqual(reader.get("/z"), Representation(b"primary-z2", False))
            self.assertIn(("/z", None), primary.seen)

    def test_each_source_has_its_own_lru(self):
        clock = FakeClock(0)
        primary_routes = {
            "/a": fixed(503),
            "/b": fixed(503),
        }
        standby_routes = {
            "/a": conditional(b"standby-a", '"sa"'),
            "/b": conditional(b"standby-b", '"sb"'),
        }
        with Origin(primary_routes) as primary, Origin(standby_routes) as standby:
            reader = self.make_reader(primary, standby, clock,
                                      entries=1, failures=2, cooldown=10)
            self.assertEqual(reader.get("/a"), Representation(b"standby-a", False))
            self.assertEqual(reader.get("/b"), Representation(b"standby-b", False))
            # Cooling down now; caching /b evicted /a from the one-entry
            # standby LRU, while /b itself is still confirmed in place.
            self.assertEqual(reader.get("/b"), Representation(b"standby-b", True))
            self.assertEqual(reader.get("/a"), Representation(b"standby-a", False))
            self.assertEqual(reader.get("/b"), Representation(b"standby-b", False))
            self.assertEqual(standby.seen, [
                ("/a", None), ("/b", None), ("/b", '"sb"'),
                ("/a", None), ("/b", None)])
            self.assertEqual(primary.paths(), ["/a", "/b"])


if __name__ == "__main__":
    unittest.main()
