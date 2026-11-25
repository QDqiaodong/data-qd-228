import unittest

from reader import ConditionalReader, OriginError, Representation
from sample_origin import local_origin


class ReaderTest(unittest.TestCase):
    def test_revalidates_bytes_and_replaces_changed_entry(self):
        resources = {"/a?q=1": ('"a"', b"old")}
        with local_origin(resources) as (port, seen):
            client = ConditionalReader("127.0.0.1", port)
            self.assertEqual(client.get("/a?q=1"), Representation(b"old", False))
            self.assertEqual(client.get("/a?q=1"), Representation(b"old", True))
            resources["/a?q=1"] = ('"b"', b"new")
            self.assertEqual(client.get("/a?q=1"), Representation(b"new", False))
            self.assertEqual(client.get("/a?q=1"), Representation(b"new", True))
            self.assertEqual([tag for _, tag in seen], [None, '"a"', '"a"', '"b"'])

    def test_lru_revalidated_entry_stays_hot(self):
        resources = {"/" + x: ('"' + x + '"', x.encode()) for x in "abc"}
        with local_origin(resources) as (port, seen):
            client = ConditionalReader("127.0.0.1", port, entries=2)
            self.assertEqual([client.get(p).revalidated for p in ("/a", "/b", "/a", "/c", "/b")],
                             [False, False, True, False, False])
            self.assertEqual(seen[-1], ("/b", None))

    def test_failure_does_not_serve_stale(self):
        resources = {"/a": ('"a"', b"ok")}
        with local_origin(resources) as (port, seen):
            client = ConditionalReader("127.0.0.1", port, max_body=3)
            client.get("/a")
            for bad in [('"b"', 304), ('"b"', 503), ('"b"', b"too long")]:
                resources["/a"] = bad
                with self.assertRaises(OriginError):
                    client.get("/a")
            resources.clear()
            with self.assertRaisesRegex(OriginError, "404"):
                client.get("/a")
            resources["/a"] = ('"a"', b"ok")
            self.assertFalse(client.get("/a").revalidated)

    def test_unvalidated_200_is_not_retained(self):
        resources = {"/a": (None, b"ok")}
        with local_origin(resources) as (port, seen):
            client = ConditionalReader("127.0.0.1", port)
            self.assertFalse(client.get("/a").revalidated)
            self.assertFalse(client.get("/a").revalidated)
            self.assertEqual(seen, [("/a", None), ("/a", None)])

    def test_do_not_retain_origin_no_store(self):
        with local_origin({"/a": ('"a"', b"ok", ("Cache-Control", "no-store"))}) as (port, seen):
            client = ConditionalReader("127.0.0.1", port)
            self.assertEqual(client.get("/a"), Representation(b"ok", False))
            self.assertEqual(client.get("/a"), Representation(b"ok", False))
            self.assertEqual(seen, [("/a", None), ("/a", None)])
