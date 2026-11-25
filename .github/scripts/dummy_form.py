"""Local-only stand-in for the Google Form during this dry run."""
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs


class DummyForm(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/ready" else 404)
        self.end_headers()

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        fields = parse_qs(body.decode())
        valid = self.path == "/formResponse" and fields.get("entry.117918617")
        self.send_response(200 if valid else 400)
        self.end_headers()

    def log_message(self, *args):
        pass


HTTPServer(("127.0.0.1", 8765), DummyForm).serve_forever()
