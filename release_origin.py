"""In-process managed release origin compatible with ReleaseReader.

The origin keeps a small, bounded set of whole configuration releases and
serves the active one through the same manifest/member protocol that
``reader.ReleaseReader`` already installs from.  Management methods are meant
to be called serially; HTTP GETs may overlap them and always observe either
the state before or after a switch, never a partial one.
"""
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import itertools
import json
import re
import threading


__all__ = ["ReleaseCapacityError", "ReleaseOrigin"]


MANIFEST_TARGET = "/release/manifest.json"

_MIB = 1024 * 1024
_MEMBER_LIMIT = _MIB
_MAX_MEMBERS = 64
_VERSION_MAX = (1 << 63) - 1
_MAX_BYTES_LIMIT = 512 * _MIB
_MIN_VERSIONS = 2
_MAX_VERSIONS = 8

_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")


class ReleaseCapacityError(RuntimeError):
    """No retention plan can satisfy both origin quotas for this switch."""


def _strict_int(value, name, lower, upper):
    """Accept only real integers in the inclusive range (never bool)."""
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError(
            f"{name} must be an integer in [{lower}, {upper}] (not bool)")
    return value


@dataclass(frozen=True)
class _MemberSpec:
    target: str
    size: int
    sha256: str


@dataclass(frozen=True)
class _Release:
    version: int
    # Sorted (logical name, spec) pairs; this ordering is only cosmetic.
    members: tuple

    def targets(self):
        return {spec.target for _name, spec in self.members}

class ReleaseOrigin:
    """Serve a managed set of releases over loopback HTTP.

    ``max_bytes`` bounds the sum of distinct member bytes referenced by all
    retained releases.  ``max_versions`` bounds the number of retained
    releases.  Both are retention quotas for the origin, not limits on the
    bytes a caller may pass to :meth:`publish`.
    """

    def __init__(self, *, max_bytes, max_versions=8):
        self._max_bytes = _strict_int(
            max_bytes, "max_bytes", 0, _MAX_BYTES_LIMIT)
        self._max_versions = _strict_int(
            max_versions, "max_versions", _MIN_VERSIONS, _MAX_VERSIONS)

        self._lock = threading.RLock()
        self._releases = {}       # version -> _Release (retained only)
        self._holds = set()       # retained versions explicitly protected
        self._active = None       # current published version, if any
        self._serving = {}        # target -> immutable bytes currently served
        self._closed = False

        origin = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self):
                origin._handle_get(self)

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        # The listening socket is live as soon as the instance exists; the
        # context manager only delimits when close() is called.
        self._thread.start()

    # ------------------------------------------------------------------
    # Context manager, identity, and read-only views
    # ------------------------------------------------------------------

    def __enter__(self):
        if self._closed:
            raise RuntimeError("ReleaseOrigin is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    @property
    def port(self):
        return self._port

    @property
    def manifest_target(self):
        return MANIFEST_TARGET

    @property
    def active_version(self):
        with self._lock:
            return self._active

    def retained_versions(self):
        with self._lock:
            return tuple(sorted(self._releases))

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._server.shutdown()
        self._thread.join(5)
        self._server.server_close()
        with self._lock:
            self._releases.clear()
            self._holds.clear()
            self._serving.clear()
            self._active = None

    # ------------------------------------------------------------------
    # Management operations
    # ------------------------------------------------------------------

    def publish(self, version, members):
        """Publish a whole configuration set and return its version."""
        self._require_open()
        version = _strict_int(version, "version", 1, _VERSION_MAX)
        specs, target_bytes = self._prepare_members(members)
        release = _Release(version, specs)

        with self._lock:
            self._require_open()
            if self._active is not None and version <= self._active:
                raise ValueError(
                    "version must be greater than the active version")

            required = set(self._holds)
            if self._active is not None:
                required.add(self._active)
            keep = self._select_retained(release, required, target_bytes)
            self._install(release, keep, target_bytes)
        return version
    def hold(self, version):
        """Protect a retained version from capacity-driven cleanup."""
        self._require_open()
        version = _strict_int(version, "version", 1, _VERSION_MAX)
        with self._lock:
            self._require_open()
            if version not in self._releases:
                raise KeyError(version)
            self._holds.add(version)

    def unhold(self, version):
        """Clear the retention mark; a no-op when it is already clear."""
        self._require_open()
        version = _strict_int(version, "version", 1, _VERSION_MAX)
        with self._lock:
            self._require_open()
            if version not in self._releases:
                raise KeyError(version)
            self._holds.discard(version)

    def restore(self, source_version, new_version):
        """Re-publish a retained version's original members under a new number."""
        self._require_open()
        source_version = _strict_int(
            source_version, "source_version", 1, _VERSION_MAX)
        new_version = _strict_int(
            new_version, "new_version", 1, _VERSION_MAX)

        with self._lock:
            self._require_open()
            if self._active is not None and new_version <= self._active:
                raise ValueError(
                    "new_version must be greater than the active version")
            source = self._releases.get(source_version)
            if source is None:
                raise KeyError(source_version)

            # Reuse the exact immutable member specs; the bytes are already
            # held by the source version, which must survive this operation.
            release = _Release(new_version, source.members)
            required = set(self._holds)
            required.add(source_version)
            if self._active is not None:
                required.add(self._active)
            keep = self._select_retained(release, required, {})
            self._install(release, keep, {})
        return new_version

    # ------------------------------------------------------------------
    # Validation and admission
    # ------------------------------------------------------------------

    def _require_open(self):
        if self._closed:
            raise RuntimeError("ReleaseOrigin is closed")

    @staticmethod
    def _prepare_members(members):
        if not isinstance(members, dict):
            raise ValueError("members must be a dictionary")
        if not 1 <= len(members) <= _MAX_MEMBERS:
            raise ValueError("members must contain 1..64 entries")

        names = list(members)
        for name in names:
            if type(name) is not str or _NAME_RE.fullmatch(name) is None:
                raise ValueError(
                    "member names must match "
                    "[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")

        specs = []
        target_bytes = {}
        for name in sorted(names):
            body = members[name]
            if not isinstance(body, bytes):
                raise ValueError(f"member {name!r} must be immutable bytes")
            body = bytes(body)
            if len(body) > _MEMBER_LIMIT:
                raise ValueError(f"member {name!r} exceeds 1 MiB")
            digest = hashlib.sha256(body).hexdigest()
            target = f"/members/{name}/{digest}"
            specs.append((name, _MemberSpec(target, len(body), digest)))
            target_bytes[target] = body
        return tuple(specs), target_bytes

    def _select_retained(self, new_release, required, new_target_bytes):
        """Choose the smallest historical deletion set that fits both quotas."""
        old_numbers = tuple(sorted(self._releases))
        old_set = set(old_numbers)
        if not required <= old_set:
            raise RuntimeError("required retention set is inconsistent")

        eligible = [number for number in old_numbers
                    if number not in required]

        # Deleting fewer versions is always preferred; ties are arbitrary.
        for delete_count in range(len(eligible) + 1):
            for removed in itertools.combinations(eligible, delete_count):
                removed_set = set(removed)
                keep = tuple(
                    number for number in old_numbers
                    if number not in removed_set)
                if len(keep) + 1 > self._max_versions:
                    continue

                needed = set(new_release.targets())
                for number in keep:
                    needed.update(self._releases[number].targets())
                total = 0
                for target in needed:
                    body = self._serving.get(target)
                    if body is None:
                        body = new_target_bytes.get(target)
                    if body is None:
                        raise RuntimeError(
                            "missing bytes for retained member target")
                    total += len(body)
                if total <= self._max_bytes:
                    return keep

        raise ReleaseCapacityError(
            "no retention plan fits max_bytes and max_versions")

    def _install(self, new_release, keep, new_target_bytes):
        """Atomically publish the new manifest, members, and retained set."""
        needed = set(new_release.targets())
        for number in keep:
            needed.update(self._releases[number].targets())

        serving = {}
        for target in needed:
            body = self._serving.get(target)
            if body is None:
                body = new_target_bytes.get(target)
            if body is None:
                # A retained historical target must already be servable.
                raise RuntimeError("missing bytes for retained member target")
            serving[target] = body

        # All admission decisions are complete; expose the new state at once.
        self._releases = {
            number: self._releases[number] for number in keep}
        self._releases[new_release.version] = new_release
        self._holds.intersection_update(self._releases)
        self._serving = serving
        self._active = new_release.version

    # ------------------------------------------------------------------
    # HTTP serving
    # ------------------------------------------------------------------

    def _handle_get(self, handler):
        path = handler.path
        if path == MANIFEST_TARGET:
            with self._lock:
                release = self._releases.get(self._active)
                body = (self._manifest_bytes(release)
                        if release is not None else None)
            self._respond(handler, body, "application/json")
            return

        with self._lock:
            body = self._serving.get(path)
        self._respond(handler, body, "application/octet-stream")

    @staticmethod
    def _manifest_bytes(release):
        members = [
            {
                "target": spec.target,
                "size": spec.size,
                "sha256": spec.sha256,
            }
            for _name, spec in release.members
        ]
        value = {
            "version": release.version,
            "serve_min": 0,
            "members": members,
        }
        return json.dumps(
            value, separators=(",", ":"), ensure_ascii=True).encode("ascii")

    @staticmethod
    def _respond(handler, body, content_type):
        if body is None:
            handler.send_response(404)
            handler.send_header("Content-Length", "0")
            handler.end_headers()
            return

        # ``body`` is an immutable snapshot selected under the lock.  A
        # subsequent switch may drop the origin's reference, but this
        # response still writes the complete selected bytes.
        handler.send_response(200)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        if body:
            try:
                handler.wfile.write(body)
            except OSError:
                pass
