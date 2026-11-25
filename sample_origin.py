"""Self-authored test origin with real wire exchanges on loopback."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread


@contextmanager
def local_origin(resources):
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            validator = self.headers.get("If-None-Match")
            seen.append((self.path, validator))
            row = resources.get(self.path)
            if row is None:
                self.send_error(404)
                return
            tag, body, *extra_headers = row
            if isinstance(body, int):
                self.send_response(body)
            elif validator == tag and tag is not None:
                self.send_response(304)
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
            if tag is not None:
                self.send_header("ETag", tag)
            for name, value in extra_headers:
                self.send_header(name, value)
            self.end_headers()
            if isinstance(body, bytes) and (validator != tag or tag is None):
                self.wfile.write(body)

        def log_message(self, *_):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": .02}, daemon=True)
        thread.start()
        try:
            yield server.server_port, seen
        finally:
            server.shutdown()
            thread.join()
