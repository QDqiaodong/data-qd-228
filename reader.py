"""Bounded client-side conditional GET for a managed origin."""
from collections import OrderedDict
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPException
import re
from urllib.parse import urlsplit


class OriginError(RuntimeError):
    pass


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
