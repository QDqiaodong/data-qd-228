"""Standard-library tests for the paged binary-object PagedReader."""
import re
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from reader import OriginError, PageBudgetExceeded, PagedReader


PAGE_SIZE = 4096
PAGE_COUNT = 64
OBJECT_SIZE = PAGE_COUNT * PAGE_SIZE
TARGET = "/rules/index.bin?table=main"
ETAG = '"v1-strong-tag"'

_RANGE_RE = re.compile(r"bytes=([0-9]+)-([0-9]+)\Z")


def page_bytes(page, tag=ETAG):
    """Distinct, deterministic bytes for every page of one object version."""
    marker = f"page={page:04d};tag={tag};".encode("ascii")
    return (marker + b"\x00" * (PAGE_SIZE - len(marker)))


def object_bytes(tag=ETAG):
    return b"".join(page_bytes(page, tag) for page in range(PAGE_COUNT))


class PagedOrigin:
    """Loopback origin serving one paged object and recording every request."""

    def __init__(self, tag=ETAG, size=OBJECT_SIZE):
        self.tag = tag
        self.size = size
        self.requests = []
        self.scripts = []
        self.lock = threading.Lock()
        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self):
                with origin.lock:
                    origin.requests.append({
                        "target": self.path,
                        "range": self.headers.get("Range"),
                        "if_match": self.headers.get("If-Match"),
                    })
                    script = origin.scripts.pop(0) if origin.scripts else None

                if self.path != TARGET:
                    body = b"not found"
                    self.send_response(404)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if self.headers.get("If-Match") != origin.tag:
                    self.send_response(412)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                parsed = _RANGE_RE.fullmatch(self.headers.get("Range") or "")
                if parsed is None:
                    self.send_response(400)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                first, last = int(parsed.group(1)), int(parsed.group(2))

                if script == "412":
                    self.send_response(412)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                if script == "ignore-range":
                    body = object_bytes(origin.tag)
                    self.send_response(200)
                    self.send_header("ETag", origin.tag)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if script == "wrong-range":
                    length = last - first + 1
                    body = object_bytes(origin.tag)[first:first + length]
                    self.send_response(206)
                    self.send_header("ETag", origin.tag)
                    self.send_header(
                        "Content-Range",
                        f"bytes {first}-{last}/{origin.size + 1}")
                    self.send_header("Content-Length", str(length))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if script == "truncated":
                    length = last - first + 1
                    self.send_response(206)
                    self.send_header("ETag", origin.tag)
                    self.send_header(
                        "Content-Range",
                        f"bytes {first}-{last}/{origin.size}")
                    self.send_header("Content-Length", str(length))
                    self.end_headers()
                    self.wfile.write(
                        object_bytes(origin.tag)[first:first + length // 2])
                    return

                if script == "duplicate-etag":
                    length = last - first + 1
                    body = object_bytes(origin.tag)[first:last + 1]
                    self.send_response(206)
                    self.send_header("ETag", origin.tag)
                    self.send_header("ETag", origin.tag)
                    self.send_header(
                        "Content-Range",
                        f"bytes {first}-{last}/{origin.size}")
                    self.send_header("Content-Length", str(length))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if script == "transfer-encoding":
                    body = object_bytes(origin.tag)[first:last + 1]
                    self.send_response(206)
                    self.send_header("ETag", origin.tag)
                    self.send_header(
                        "Content-Range",
                        f"bytes {first}-{last}/{origin.size}")
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    self.wfile.write(body)
                    return

                body = object_bytes(origin.tag)[first:last + 1]
                self.send_response(206)
                self.send_header("ETag", origin.tag)
                self.send_header(
                    "Content-Range", f"bytes {first}-{last}/{origin.size}")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
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

    def reset(self):
        with self.lock:
            self.requests.clear()
            self.scripts.clear()

    def script(self, *names):
        with self.lock:
            self.scripts.extend(names)

    @property
    def get_count(self):
        with self.lock:
            return len(self.requests)

    def segment_pages(self):
        result = []
        with self.lock:
            for request in self.requests:
                match = _RANGE_RE.fullmatch(request["range"])
                first, last = int(match.group(1)), int(match.group(2))
                result.append((first // PAGE_SIZE, last // PAGE_SIZE))
        return result

    def body_page_count(self):
        return sum(end - start + 1 for start, end in self.segment_pages())

    def assert_headers(self, test_case):
        with self.lock:
            for request in self.requests:
                test_case.assertEqual(request["target"], TARGET)
                test_case.assertIsNotNone(
                    _RANGE_RE.fullmatch(request["range"] or ""))
                test_case.assertEqual(request["if_match"], ETAG)


def make_reader(origin, **kwargs):
    return PagedReader(
        "127.0.0.1", origin.port, TARGET,
        size=OBJECT_SIZE, etag=ETAG, **kwargs)


def assert_page(test_case, data, page, tag=ETAG):
    test_case.assertEqual(data, page_bytes(page, tag))


class RetentionAndBudgetTest(unittest.TestCase):
    def test_duplicate_pages_and_alternative_budgets(self):
        # (a) 1 GET / 4 page bytes: one merged segment 40..43.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            result = reader.read_pages(
                [43, 40, 43], max_gets=1, max_body_bytes=4 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 1)
            self.assertEqual(origin.segment_pages(), [(40, 43)])
            origin.assert_headers(self)
            assert_page(self, result[0], 43)
            assert_page(self, result[1], 40)
            assert_page(self, result[2], 43)
            reader.close()

        # (b) 2 GET / 2 page bytes: two single-page GETs, 40 and 43.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            result = reader.read_pages(
                [43, 40, 43], max_gets=2, max_body_bytes=2 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 2)
            self.assertEqual(origin.body_page_count(), 2)
            self.assertEqual(
                sorted(origin.segment_pages()), [(40, 40), (43, 43)])
            origin.assert_headers(self)
            assert_page(self, result[0], 43)
            assert_page(self, result[1], 40)
            assert_page(self, result[2], 43)
            reader.close()

        # (c) 2 GET / 4 page bytes: either plan is valid; account for the
        # requests that were actually sent.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            result = reader.read_pages(
                [43, 40, 43], max_gets=2, max_body_bytes=4 * PAGE_SIZE)
            self.assertLessEqual(origin.get_count, 2)
            self.assertLessEqual(origin.body_page_count(), 4)
            for start, end in origin.segment_pages():
                self.assertLessEqual(end - start + 1, 8)
            origin.assert_headers(self)
            assert_page(self, result[0], 43)
            assert_page(self, result[1], 40)
            assert_page(self, result[2], 43)
            reader.close()

        # (d) 1 GET / 1 page byte: rejected before any GET.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages(
                    [43, 40, 43], max_gets=1, max_body_bytes=PAGE_SIZE)
            self.assertEqual(origin.get_count, 0)

            # Retain page 40, then repeat the tight budget: only 43 is fetched.
            retained = reader.read_pages(
                [40], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            assert_page(self, retained[0], 40)
            origin.reset()

            result = reader.read_pages(
                [43, 40, 43], max_gets=1, max_body_bytes=PAGE_SIZE)
            self.assertEqual(origin.get_count, 1)
            self.assertEqual(origin.segment_pages(), [(43, 43)])
            origin.assert_headers(self)
            assert_page(self, result[0], 43)
            assert_page(self, result[1], 40)
            assert_page(self, result[2], 43)
            reader.close()

        # (e) All hits: zero origin contact even with zero budgets.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [40, 43], max_gets=1, max_body_bytes=4 * PAGE_SIZE, keep=True)
            origin.reset()
            result = reader.read_pages(
                [43, 40, 43], max_gets=0, max_body_bytes=0)
            self.assertEqual(origin.get_count, 0)
            assert_page(self, result[0], 43)
            assert_page(self, result[1], 40)
            assert_page(self, result[2], 43)
            reader.close()

        # (f) Full retention set: new keep batch rejected, old pages still read.
        with PagedOrigin() as origin:
            reader = make_reader(origin, retained_pages=2)
            reader.read_pages(
                [1, 2], max_gets=1, max_body_bytes=2 * PAGE_SIZE, keep=True)
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages(
                    [3], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            origin.reset()
            result = reader.read_pages([2, 1], max_gets=0, max_body_bytes=0)
            self.assertEqual(origin.get_count, 0)
            assert_page(self, result[0], 2)
            assert_page(self, result[1], 1)
            reader.close()

        # (g) Pages received incidentally inside a merged segment are not kept.
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [40, 43], max_gets=1, max_body_bytes=4 * PAGE_SIZE, keep=True)
            self.assertEqual(origin.segment_pages(), [(40, 43)])

        # Same instance continues below after the origin is torn down.
        with PagedOrigin() as origin:
            offline_reader = make_reader(origin)
            offline_reader.read_pages(
                [40, 43], max_gets=1, max_body_bytes=4 * PAGE_SIZE,
                keep=True)
        # Origin is now closed: retained pages survive, incidental pages do not.
        retained = offline_reader.read_pages(
            [43, 40], max_gets=0, max_body_bytes=0)
        assert_page(self, retained[0], 43)
        assert_page(self, retained[1], 40)
        with self.assertRaises(PageBudgetExceeded):
            offline_reader.read_pages([41], max_gets=0, max_body_bytes=0)
        with self.assertRaises(PageBudgetExceeded):
            offline_reader.read_pages([42], max_gets=0, max_body_bytes=0)
        offline_reader.close()

    def test_retained_pages_survive_origin_version_swap(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [7], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            origin.reset()

            # The origin now serves a different object version.
            origin.tag = '"v2-other-tag"'
            result = reader.read_pages([7], max_gets=0, max_body_bytes=0)
            self.assertEqual(origin.get_count, 0)
            assert_page(self, result[0], 7, ETAG)

            # A missing page cannot be fetched: If-Match no longer matches.
            with self.assertRaises(OriginError):
                reader.read_pages(
                    [8], max_gets=1, max_body_bytes=PAGE_SIZE)
            reader.close()


class SegmentPlanningTest(unittest.TestCase):
    def test_two_segments_with_holes_and_eight_page_cap(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            result = reader.read_pages(
                [2, 8, 12, 18], max_gets=2, max_body_bytes=14 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 2)
            self.assertEqual(origin.body_page_count(), 14)
            self.assertEqual(
                sorted(origin.segment_pages()), [(2, 8), (12, 18)])
            origin.assert_headers(self)
            for data, page in zip(result, [2, 8, 12, 18]):
                assert_page(self, data, page)
            reader.close()

    def test_one_page_less_of_budget_rejects_without_requests(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages(
                    [2, 8, 12, 18],
                    max_gets=2, max_body_bytes=13 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 0)
            reader.close()


class FailedBatchRetentionTest(unittest.TestCase):
    def test_412_on_second_get_fails_whole_keep_batch(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [2], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            origin.reset()

            # Pages 5 and 11 are two non-adjacent missing pages.
            origin.script("206", "412")
            with self.assertRaises(OriginError):
                reader.read_pages(
                    [5, 11], max_gets=2, max_body_bytes=2 * PAGE_SIZE,
                    keep=True)
            self.assertEqual(origin.get_count, 2)

            # Zero origin budget: the old retained page still reads; the page
            # received from the first (successful) 206 was never retained.
            old = reader.read_pages([2], max_gets=0, max_body_bytes=0)
            assert_page(self, old[0], 2)
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages([5], max_gets=0, max_body_bytes=0)
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages([11], max_gets=0, max_body_bytes=0)
            reader.close()

    def test_malformed_responses_never_form_retention(self):
        for script in ("wrong-range", "truncated", "ignore-range",
                       "duplicate-etag", "transfer-encoding"):
            with PagedOrigin() as origin:
                reader = make_reader(origin)
                origin.script(script)
                with self.assertRaises(OriginError):
                    reader.read_pages(
                        [5], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
                self.assertEqual(origin.get_count, 1)
                with self.assertRaises(PageBudgetExceeded):
                    reader.read_pages([5], max_gets=0, max_body_bytes=0)
                self.assertEqual(reader._retained, {})
                reader.close()

    def test_segment_crossing_retained_page_pays_full_span(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [5], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            origin.reset()

            # One GET covering pages 4..7 must download all four pages even
            # though page 5 is already local: a three-page budget cannot fit.
            with self.assertRaises(PageBudgetExceeded):
                reader.read_pages(
                    [4, 7], max_gets=1, max_body_bytes=3 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 0)

            result = reader.read_pages(
                [4, 7], max_gets=1, max_body_bytes=4 * PAGE_SIZE)
            self.assertEqual(origin.get_count, 1)
            self.assertEqual(origin.segment_pages(), [(4, 7)])
            self.assertEqual(origin.body_page_count(), 4)
            assert_page(self, result[0], 4)
            assert_page(self, result[1], 7)
            reader.close()


class ValidationAndLifecycleTest(unittest.TestCase):
    def test_constructor_validates_arguments_without_network(self):
        good_kwargs = dict(size=OBJECT_SIZE, etag=ETAG)
        for bad_size in (0, PAGE_SIZE - 1, 256 * 1024 * 1024 + PAGE_SIZE,
                         OBJECT_SIZE + 1, True):
            with self.assertRaises(ValueError):
                PagedReader("127.0.0.1", 0, TARGET,
                            size=bad_size, etag=ETAG)
        for bad_etag in ('', 'v1', '"unterminated', 'W/"weak"', 7):
            with self.assertRaises(ValueError):
                PagedReader("127.0.0.1", 0, TARGET,
                            size=OBJECT_SIZE, etag=bad_etag)
        for bad_retained in (-1, 65, True, 1.0):
            with self.assertRaises(ValueError):
                PagedReader("127.0.0.1", 0, TARGET,
                            retained_pages=bad_retained, **good_kwargs)
        for bad_target in ("relative", "//host/x", "/x#f", "/x y",
                           "http://127.0.0.1/x", "/" + "a" * 256):
            with self.assertRaises(ValueError):
                PagedReader("127.0.0.1", 0, bad_target, **good_kwargs)

        # Construction does not open a connection.
        PagedReader("127.0.0.1", 1, TARGET, **good_kwargs).close()

    def test_read_pages_validates_arguments_without_side_effects(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            for bad_pages in ("x", {0}, set()):
                with self.assertRaises(ValueError):
                    reader.read_pages(
                        bad_pages, max_gets=0, max_body_bytes=0)
            with self.assertRaises(ValueError):
                reader.read_pages(
                    [0] * 33, max_gets=32, max_body_bytes=0)
            with self.assertRaises(ValueError):
                reader.read_pages(
                    [PAGE_COUNT], max_gets=1, max_body_bytes=PAGE_SIZE)
            with self.assertRaises(ValueError):
                reader.read_pages([-1], max_gets=1,
                                  max_body_bytes=PAGE_SIZE)
            with self.assertRaises(ValueError):
                reader.read_pages([True], max_gets=1,
                                  max_body_bytes=PAGE_SIZE)
            for bad_gets in (-1, 33, True, 1.0):
                with self.assertRaises(ValueError):
                    reader.read_pages(
                        [0], max_gets=bad_gets, max_body_bytes=0)
            for bad_budget in (-1, 1024 * 1024 + 1, True, 1.0):
                with self.assertRaises(ValueError):
                    reader.read_pages(
                        [0], max_gets=0, max_body_bytes=bad_budget)
            with self.assertRaises(ValueError):
                reader.read_pages(
                    [0], max_gets=0, max_body_bytes=0, keep="yes")

            # Empty batches are terminal and contact no origin.
            self.assertEqual(
                reader.read_pages([], max_gets=0, max_body_bytes=0), [])
            self.assertEqual(origin.get_count, 0)
            reader.close()

    def test_close_is_idempotent_and_reads_after_close_fail(self):
        with PagedOrigin() as origin:
            reader = make_reader(origin)
            reader.read_pages(
                [3], max_gets=1, max_body_bytes=PAGE_SIZE, keep=True)
            reader.close()
            reader.close()
            with self.assertRaises(RuntimeError):
                reader.read_pages([3], max_gets=0, max_body_bytes=0)


if __name__ == "__main__":
    unittest.main()
