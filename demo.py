from reader import ConditionalReader
from sample_origin import local_origin


if __name__ == "__main__":
    resources = {"/manifest": ('"rev1"', b"build=101")}
    with local_origin(resources) as (port, seen):
        client = ConditionalReader("127.0.0.1", port)
        first = client.get("/manifest")
        second = client.get("/manifest")
        resources["/manifest"] = ('"rev2"', b"build=102")
        third = client.get("/manifest")
        assert [r.body for r in (first, second, third)] == [b"build=101", b"build=101", b"build=102"]
        assert [r.revalidated for r in (first, second, third)] == [False, True, False]
        assert [tag for _, tag in seen] == [None, '"rev1"', '"rev1"']
        for result in (first, second, third):
            print("304" if result.revalidated else "200", result.body.decode("ascii"))
