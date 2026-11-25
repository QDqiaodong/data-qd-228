"""Standard-library concurrency tests for ScheduledReader."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import time
import unittest

from reader import (
    OriginError,
    QueueFull,
    Representation,
    RequestCancelled,
    ScheduledReader,
)


def response(status=200, body=b"", etag=None, extra_headers=()):
    headers = list(extra_headers)
    if etag is not None:
        headers.append(("ETag", etag))
    if status == 200:
        headers.append(("Content-Length", str(len(body))))
    return status, headers, body


class Call:
    def __init__(self, seq, method_path, validator, headers):
        self.seq = seq
        self.path = method_path
        self.validator = validator
        self.headers = headers
        self.release = threading.Event()
        self._response = None

    def respond(self, spec):
        self._response = spec
        self.release.set()

    @property
    def response_spec(self):
        return self._response


class ScriptedOrigin:
    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls = []
        self.cond = threading.Condition()
        origin = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                validator = self.headers.get("If-None-Match")
                headers = {k: v for k, v in self.headers.items()}
                call = Call(len(origin.calls), self.path, validator, headers)
                with origin.cond:
                    origin.calls.append(call)
                    origin.cond.notify_all()

                if not call.release.wait(10):
                    spec = response(500, b"test release timeout")
                else:
                    spec = call.response_spec or origin.routes.get(self.path)
                    if spec is None:
                        spec = response(404, b"no scripted response")

                status, response_headers, body = spec
                self.send_response(status)
                has_length = any(k.lower() == "content-length"
                                 for k, _ in response_headers)
                for name, value in response_headers:
                    self.send_header(name, value)
                if status == 200 and not has_length:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if status == 200 and body:
                    self.wfile.write(body)

            def log_message(self, *_):
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
        self.start()
        return self

    def start(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        for call in self.calls:
            call.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        return False

    def wait_for_calls(self, count, timeout=5.0):
        deadline = time.monotonic() + timeout
        with self.cond:
            while len(self.calls) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"expected {count} requests, saw {len(self.calls)}")
                self.cond.wait(remaining)
            return list(self.calls)

    def wait_for_path_count(self, path, count=1, timeout=5.0):
        deadline = time.monotonic() + timeout
        with self.cond:
            while sum(c.path == path for c in self.calls) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    seen = [(c.path, c.validator) for c in self.calls]
                    raise AssertionError(
                        f"expected {count} calls for {path}; saw {seen}")
                self.cond.wait(remaining)
            return [c for c in self.calls if c.path == path]

    @property
    def seen(self):
        return [(call.path, call.validator) for call in self.calls]


class FakeClock:
    def __init__(self, value=0.0):
        self.value = float(value)
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, seconds):
        with self.lock:
            self.value += seconds


class ResultThread(threading.Thread):
    def __init__(self, ticket):
        super().__init__(daemon=True)
        self.ticket = ticket
        self.value = None
        self.error = None

    def run(self):
        try:
            self.value = self.ticket.result()
        except BaseException as exc:  # captured for asserting thread
            self.error = exc

    def assert_done(self, test_case, timeout=5.0):
        self.join(timeout)
        test_case.assertFalse(self.is_alive(), "ticket.result() did not finish")
        return self.value, self.error


class ScheduledValidationTest(unittest.TestCase):
    def test_constructor_arguments_are_strict(self):
        for name, value in [
            ("parallelism", 0), ("parallelism", 9),
            ("parallelism", True), ("parallelism", 2.0),
            ("max_pending", -1), ("max_pending", 65),
            ("max_pending", False),
            ("max_waiters", 0), ("max_waiters", 129),
            ("max_waiters", True),
        ]:
            kwargs = {name: value}
            with self.assertRaises(ValueError):
                ScheduledReader("127.0.0.1", 0, **kwargs)

    def test_target_and_deadline_validation_has_no_side_effects(self):
        clock = FakeClock(10)
        reader = ScheduledReader("127.0.0.1", 1, clock=clock)
        try:
            for bad in ["absolute", "http://x/a", "//x/a", "/a#f", "/a b"]:
                with self.assertRaises(ValueError):
                    reader.submit(bad)
            for bad in [True, False, float("nan"), float("inf"), "12", 1j]:
                with self.assertRaises(ValueError):
                    reader.submit("/a", deadline=bad)
        finally:
            reader.close()

    def test_expired_submission_is_terminal_and_consumes_no_capacity(self):
        clock = FakeClock(100)
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, max_waiters=1, clock=clock)
            try:
                tickets = [reader.submit("/a", deadline=99) for _ in range(4)]
                for ticket in tickets:
                    with self.assertRaises(TimeoutError):
                        ticket.result()
                    self.assertFalse(ticket.cancel())

                live = reader.submit("/a", deadline=101)
                call = origin.wait_for_calls(1)[0]
                call.respond(response(200, b"a", '"a"'))
                self.assertEqual(live.result(), Representation(b"a", False))
                self.assertEqual(origin.seen, [("/a", None)])
            finally:
                reader.close()

    def test_closed_reader_rejects_submission_and_close_repeats(self):
        reader = ScheduledReader("127.0.0.1", 1)
        reader.close()
        reader.close()
        with self.assertRaises(RuntimeError):
            reader.submit("/a")


class SharedGetAndDeadlineTest(unittest.TestCase):
    def test_two_tickets_share_conditional_get_and_expiry_is_independent(self):
        clock = FakeClock(100)
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, clock=clock, timeout=2)
            try:
                warm = reader.submit("/cfg")
                first = origin.wait_for_calls(1)[0]
                first.respond(response(200, b"old-bytes", '"v1"'))
                self.assertEqual(
                    warm.result(), Representation(b"old-bytes", False))

                expired_ticket = reader.submit("/cfg", deadline=105)
                live_ticket = reader.submit("/cfg")
                second = origin.wait_for_calls(2)[1]
                self.assertEqual(second.validator, '"v1"')

                expired_run = ResultThread(expired_ticket)
                live_run = ResultThread(live_ticket)
                expired_run.start()
                live_run.start()

                clock.advance(5)
                with self.assertRaises(TimeoutError):
                    expired_ticket.result()
                _, error = expired_run.assert_done(self)
                self.assertIsInstance(error, TimeoutError)
                self.assertFalse(expired_ticket.cancel())

                # Transfer is still blocked at this point.
                self.assertEqual(len(origin.calls), 2)
                second.respond(response(304, b"", '"v1"'))
                value, error = live_run.assert_done(self)
                self.assertIsNone(error)
                self.assertEqual(value, Representation(b"old-bytes", True))

                # The confirmed validator remains usable by later reads.
                later = reader.submit("/cfg")
                third = origin.wait_for_calls(3)[2]
                self.assertEqual(third.validator, '"v1"')
                third.respond(response(304, b"", '"v1"'))
                self.assertEqual(
                    later.result(), Representation(b"old-bytes", True))
                self.assertEqual(
                    origin.seen,
                    [("/cfg", None), ("/cfg", '"v1"'), ("/cfg", '"v1"')])
            finally:
                reader.close()

    def test_completion_registration_uses_current_fake_clock(self):
        clock = FakeClock(0)
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, clock=clock)
            try:
                warm = reader.submit("/cfg")
                warm_call = origin.wait_for_calls(1)[0]
                warm_call.respond(response(200, b"bytes", '"v"'))
                warm.result()

                expired = reader.submit("/cfg", deadline=5)
                live = reader.submit("/cfg")
                conditional = origin.wait_for_calls(2)[1]
                self.assertEqual(conditional.validator, '"v"')

                # No result() thread is polling the expired ticket.  Advancing
                # the clock and releasing the response makes completion
                # registration perform the expiration.
                clock.advance(5)
                conditional.respond(response(304, etag='"v"'))

                self.assertEqual(
                    live.result(), Representation(b"bytes", True))
                with self.assertRaises(TimeoutError):
                    expired.result()
                self.assertEqual(len(origin.calls), 2)
            finally:
                reader.close()

    def test_real_clock_result_expires_while_transfer_is_blocked(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, parallelism=1, timeout=2)
            try:
                started = time.monotonic()
                ticket = reader.submit("/slow", deadline=started + 0.15)
                call = origin.wait_for_calls(1)[0]
                with self.assertRaises(TimeoutError):
                    ticket.result()
                elapsed = time.monotonic() - started
                self.assertGreaterEqual(elapsed, 0.12)
                self.assertLess(elapsed, 1.5)
                self.assertEqual(len(origin.calls), 1)
                for _ in range(2):
                    with self.assertRaises(TimeoutError):
                        ticket.result()
                self.assertFalse(ticket.cancel())
                call.respond(response(200, b"late", '"late"'))
            finally:
                reader.close()

    def test_exact_query_and_path_are_distinct(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader("127.0.0.1", origin.port)
            try:
                t1 = reader.submit("/cfg?x=1")
                t2 = reader.submit("/cfg?x=2")
                calls = origin.wait_for_calls(2)
                for call in calls:
                    call.respond(response(200, call.path.encode(), '"t"'))
                self.assertEqual(t1.result().body, b"/cfg?x=1")
                self.assertEqual(t2.result().body, b"/cfg?x=2")
                self.assertEqual(len(origin.calls), 2)
            finally:
                reader.close()


class CancellationCacheTest(unittest.TestCase):
    def test_cancelled_old_get_does_not_feed_or_clear_next_work(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, parallelism=2)
            try:
                warm = reader.submit("/same")
                warm_call = origin.wait_for_calls(1)[0]
                warm_call.respond(response(200, b"old-bytes", '"old"'))
                self.assertEqual(warm.result(), Representation(b"old-bytes", False))

                cancelled = reader.submit("/same")
                old_call = origin.wait_for_path_count("/same", 2)[1]
                self.assertEqual(old_call.validator, '"old"')
                self.assertTrue(cancelled.cancel())
                with self.assertRaises(RequestCancelled):
                    cancelled.result()
                self.assertFalse(cancelled.cancel())

                replacement = reader.submit("/same")
                replacement_run = ResultThread(replacement)
                replacement_run.start()

                other = reader.submit("/other")
                other_call = origin.wait_for_path_count("/other", 1)[0]
                other_call.respond(response(200, b"other", '"other"'))
                self.assertEqual(other.result(), Representation(b"other", False))

                # The late 200 belongs to the cancelled work and must not be
                # delivered to replacement or replace its cached validator.
                old_call.respond(response(200, b"late-bytes", '"late"'))
                new_call = origin.wait_for_path_count("/same", 3)[2]
                self.assertEqual(new_call.validator, '"old"')
                new_call.respond(response(304, b"", '"old"'))

                value, error = replacement_run.assert_done(self)
                self.assertIsNone(error)
                self.assertEqual(value, Representation(b"old-bytes", True))

                # A 404 from another no-longer-valid old request likewise must
                # not clear the cache used by the next request.
                doomed = reader.submit("/same")
                not_found_call = origin.wait_for_path_count("/same", 4)[3]
                self.assertEqual(not_found_call.validator, '"old"')
                self.assertTrue(doomed.cancel())
                not_found_call.respond(response(404, b"gone"))

                after_404 = reader.submit("/same")
                after_call = origin.wait_for_path_count("/same", 5)[4]
                self.assertEqual(after_call.validator, '"old"')
                after_call.respond(response(200, b"after", '"after"'))
                self.assertEqual(after_404.result(),
                                 Representation(b"after", False))

                self.assertEqual(
                    [v for p, v in origin.seen if p == "/same"],
                    [None, '"old"', '"old"', '"old"', '"old"'])
            finally:
                reader.close()

    def test_timed_out_last_ticket_late_200_does_not_touch_cache(self):
        clock = FakeClock(0)
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, clock=clock)
            try:
                warm = reader.submit("/same")
                warm_call = origin.wait_for_calls(1)[0]
                warm_call.respond(response(200, b"old", '"old"'))
                warm.result()

                timed_out = reader.submit("/same", deadline=5)
                old_call = origin.wait_for_path_count("/same", 2)[1]
                clock.advance(5)
                old_call.respond(response(200, b"late", '"late"'))

                with self.assertRaises(TimeoutError):
                    timed_out.result()

                next_ticket = reader.submit("/same")
                next_call = origin.wait_for_path_count("/same", 3)[2]
                self.assertEqual(next_call.validator, '"old"')
                next_call.respond(response(304, etag='"old"'))
                self.assertEqual(
                    next_ticket.result(), Representation(b"old", True))
            finally:
                reader.close()

    def test_active_ticket_without_waiting_thread_still_commits_404(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader("127.0.0.1", origin.port)
            try:
                warm = reader.submit("/item")
                call = origin.wait_for_calls(1)[0]
                call.respond(response(200, b"item", '"tag"'))
                warm.result()

                # No thread is parked in result(), but the ticket is active.
                unwatched = reader.submit("/item")
                conditional = origin.wait_for_calls(2)[1]
                self.assertEqual(conditional.validator, '"tag"')
                conditional.respond(response(404, b"gone"))
                with self.assertRaisesRegex(OriginError, "404"):
                    unwatched.result()

                after = reader.submit("/item")
                next_call = origin.wait_for_calls(3)[2]
                self.assertIsNone(next_call.validator)
                next_call.respond(response(200, b"new", '"new"'))
                self.assertEqual(after.result(), Representation(b"new", False))
            finally:
                reader.close()

    def test_close_cancels_waiter_but_lets_in_flight_transfer_finish(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, parallelism=1, timeout=2)
            try:
                ticket = reader.submit("/close-me")
                call = origin.wait_for_calls(1)[0]
                runner = ResultThread(ticket)
                runner.start()

                closer = threading.Thread(target=reader.close, daemon=True)
                closer.start()

                _, error = runner.assert_done(self, timeout=2.0)
                self.assertIsInstance(error, RequestCancelled)
                self.assertFalse(ticket.cancel())
                self.assertTrue(closer.is_alive())

                call.respond(response(200, b"finished", '"t"'))
                closer.join(5)
                self.assertFalse(closer.is_alive())

                with self.assertRaises(RuntimeError):
                    reader.submit("/after-close")
            finally:
                try:
                    reader.close()
                except Exception:
                    pass


class CapacitySchedulingTest(unittest.TestCase):
    def test_merge_rejection_expiry_and_runnable_fifo(self):
        clock = FakeClock(0)
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port,
                parallelism=1, max_pending=3, max_waiters=32, clock=clock)
            try:
                running = reader.submit("/w0")
                blocked_call = origin.wait_for_calls(1)[0]

                a1 = reader.submit("/a", deadline=10)
                a2 = reader.submit("/a")
                a3 = reader.submit("/a")
                expiring = reader.submit("/exp", deadline=5)
                b1 = reader.submit("/b")
                self.assertEqual(len(origin.calls), 1)

                # pending is [a, exp, b], at its limit.
                with self.assertRaises(QueueFull):
                    reader.submit("/rejected")

                # Merging another ticket adds no work-item capacity.
                b2 = reader.submit("/b")
                self.assertTrue(b1.cancel())
                self.assertTrue(b2.cancel())
                with self.assertRaises(RequestCancelled):
                    b1.result()
                with self.assertRaises(RequestCancelled):
                    b2.result()

                c_ticket = reader.submit("/c")

                # An unwatched but expired ticket removes its queued work and
                # releases pending capacity.
                clock.advance(5)
                with self.assertRaises(TimeoutError):
                    expiring.result()

                d_ticket = reader.submit("/d")
                with self.assertRaises(QueueFull):
                    reader.submit("/rejected-again")

                blocked_call.respond(response(200, b"w0", '"w0"'))
                self.assertEqual(running.result(), Representation(b"w0", False))

                a_call = origin.wait_for_path_count("/a", 1)[0]
                a_call.respond(response(200, b"a-body", '"a"'))
                for ticket in (a1, a2, a3):
                    self.assertEqual(
                        ticket.result(), Representation(b"a-body", False))

                c_call = origin.wait_for_path_count("/c", 1)[0]
                c_call.respond(response(200, b"c-body", '"c"'))
                self.assertEqual(
                    c_ticket.result(), Representation(b"c-body", False))

                d_call = origin.wait_for_path_count("/d", 1)[0]
                d_call.respond(response(200, b"d-body", '"d"'))
                self.assertEqual(
                    d_ticket.result(), Representation(b"d-body", False))

                paths = [call.path for call in origin.calls]
                self.assertEqual(paths, ["/w0", "/a", "/c", "/d"])
            finally:
                reader.close()

    def test_waiter_capacity_is_shared_by_all_tickets(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port,
                parallelism=1, max_pending=0, max_waiters=2)
            try:
                first = reader.submit("/same")
                call = origin.wait_for_calls(1)[0]
                second = reader.submit("/same")
                with self.assertRaises(QueueFull):
                    reader.submit("/same")
                with self.assertRaises(QueueFull):
                    reader.submit("/other")

                call.respond(response(200, b"same", '"s"'))
                expected = Representation(b"same", False)
                self.assertEqual(first.result(), expected)
                self.assertEqual(second.result(), expected)
            finally:
                reader.close()

    def test_zero_pending_rejects_blocked_new_work_but_allows_immediate(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port,
                parallelism=1, max_pending=0, max_waiters=10)
            try:
                blocker = reader.submit("/block")
                call = origin.wait_for_calls(1)[0]
                with self.assertRaises(QueueFull):
                    reader.submit("/queued")

                call.respond(response(200, b"block", '"b"'))
                self.assertEqual(
                    blocker.result(), Representation(b"block", False))

                immediate = reader.submit("/immediate")
                next_call = origin.wait_for_path_count("/immediate", 1)[0]
                next_call.respond(response(200, b"now", '"n"'))
                self.assertEqual(
                    immediate.result(), Representation(b"now", False))
            finally:
                reader.close()


class LRUAndOriginErrorTest(unittest.TestCase):
    def test_in_flight_304_reinserts_bytes_evicted_by_other_target(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader(
                "127.0.0.1", origin.port, parallelism=2, entries=1)
            try:
                warm_a = reader.submit("/A")
                warm_call = origin.wait_for_calls(1)[0]
                warm_call.respond(response(200, b"A-old", '"a-tag"'))
                self.assertEqual(
                    warm_a.result(), Representation(b"A-old", False))

                revalidate_a = reader.submit("/A")
                a_call = origin.wait_for_path_count("/A", 2)[1]
                self.assertEqual(a_call.validator, '"a-tag"')

                b = reader.submit("/B")
                b_call = origin.wait_for_path_count("/B", 1)[0]
                b_call.respond(response(200, b"B-body", '"b-tag"'))
                self.assertEqual(b.result(), Representation(b"B-body", False))

                # B has evicted A from the one-entry LRU before A's 304 lands.
                a_call.respond(response(304, b"", '"a-tag"'))
                self.assertEqual(
                    revalidate_a.result(), Representation(b"A-old", True))

                next_a = reader.submit("/A")
                next_a_call = origin.wait_for_path_count("/A", 3)[2]
                self.assertEqual(next_a_call.validator, '"a-tag"')
                next_a_call.respond(response(304, b"", '"a-tag"'))
                self.assertEqual(
                    next_a.result(), Representation(b"A-old", True))
            finally:
                reader.close()

    def test_origin_error_does_not_return_stale_body(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader("127.0.0.1", origin.port)
            try:
                warm = reader.submit("/item")
                warm_call = origin.wait_for_calls(1)[0]
                warm_call.respond(response(200, b"stale", '"tag"'))
                warm.result()

                bad = reader.submit("/item")
                conditional = origin.wait_for_calls(2)[1]
                conditional.respond(response(503, b"busy"))
                with self.assertRaises(OriginError):
                    bad.result()

                after = reader.submit("/item")
                next_call = origin.wait_for_calls(3)[2]
                # Other failures neither clear nor recency-touch the cache.
                self.assertEqual(next_call.validator, '"tag"')
                next_call.respond(response(200, b"fresh", '"tag2"'))
                self.assertEqual(
                    after.result(), Representation(b"fresh", False))
            finally:
                reader.close()


class ScheduledCacheRuleTest(unittest.TestCase):
    def test_mismatched_and_non_retainable_304_clear_cache(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader("127.0.0.1", origin.port)
            try:
                warm = reader.submit("/a")
                call = origin.wait_for_calls(1)[0]
                call.respond(response(200, b"a", '"a"'))
                warm.result()

                mismatched = reader.submit("/a")
                call = origin.wait_for_calls(2)[1]
                call.respond(response(304, etag='"wrong"'))
                with self.assertRaises(OriginError):
                    mismatched.result()

                after_mismatch = reader.submit("/a")
                call = origin.wait_for_calls(3)[2]
                self.assertIsNone(call.validator)
                call.respond(response(200, b"a2", '"a2"'))
                after_mismatch.result()

                no_store = reader.submit("/a")
                call = origin.wait_for_calls(4)[3]
                self.assertEqual(call.validator, '"a2"')
                call.respond(response(
                    304, etag='"a2"',
                    extra_headers=[("Cache-Control", "no-store")]))
                value = no_store.result()
                self.assertEqual(value, Representation(b"a2", True))

                after_no_store = reader.submit("/a")
                call = origin.wait_for_calls(5)[4]
                self.assertIsNone(call.validator)
                call.respond(response(200, b"a3", '"a3"'))
                self.assertEqual(
                    after_no_store.result(), Representation(b"a3", False))
            finally:
                reader.close()

    def test_unsupported_length_keeps_previous_validator(self):
        with ScriptedOrigin() as origin:
            reader = ScheduledReader("127.0.0.1", origin.port, max_body=8)
            try:
                warm = reader.submit("/big")
                call = origin.wait_for_calls(1)[0]
                call.respond(response(200, b"1234", '"big"'))
                warm.result()

                bad = reader.submit("/big")
                call = origin.wait_for_calls(2)[1]
                call.respond((200, [("Content-Length", "999999")], b"short"))
                with self.assertRaises(OriginError):
                    bad.result()

                after = reader.submit("/big")
                call = origin.wait_for_calls(3)[2]
                self.assertEqual(call.validator, '"big"')
                call.respond(response(304, etag='"big"'))
                self.assertEqual(after.result(), Representation(b"1234", True))
            finally:
                reader.close()
