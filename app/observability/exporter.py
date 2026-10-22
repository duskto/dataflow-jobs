"""Internal-only Prometheus endpoint. Compose does not publish this port to the host."""

import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.core.config import get_settings
from app.observability.metrics import render, snapshot


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return
        try:
            data = render(snapshot(get_settings().owner_id)).encode()
        except Exception:
            logging.exception("Metrics snapshot unavailable")
            self.send_error(503, "Dependencies unavailable")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ThreadingHTTPServer(("0.0.0.0", 9108), Handler).serve_forever()
