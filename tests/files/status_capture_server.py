# Fake SLING_STATUS_URL endpoint. Prints "PORT=<n>", then one line per posted
# pipeline step status: "status step=<step_id> => <status>".
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply()

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            payload = json.loads(body)
            if payload.get("step_id"):
                print(f"status step={payload['step_id']} => {payload.get('status')}", flush=True)
        except ValueError:
            pass
        self.reply()

    def reply(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


server = HTTPServer(("127.0.0.1", 0), Handler)
print(f"PORT={server.server_address[1]}", flush=True)
server.serve_forever()
