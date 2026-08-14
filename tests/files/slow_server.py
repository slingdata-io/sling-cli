"""HTTP server that adds latency to each file download.

Emulates a high-latency file store (Google Drive) for MergeReaders tests.
Usage: python3 slow_server.py <port> <dir> <delay_secs> [<life_secs>]
"""
import http.server
import socketserver
import sys
import threading
import time

PORT = int(sys.argv[1])
DIR = sys.argv[2]
DELAY = float(sys.argv[3])
LIFE = float(sys.argv[4]) if len(sys.argv) > 4 else 300


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def do_GET(self):
        # delay file downloads only, not the index page
        if self.path.endswith(".csv"):
            time.sleep(DELAY)
        super().do_GET()

    def log_message(self, format, *args):
        pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


srv = Server(("127.0.0.1", PORT), Handler)
threading.Timer(LIFE, srv.shutdown).start()
print("ready", flush=True)
srv.serve_forever()
