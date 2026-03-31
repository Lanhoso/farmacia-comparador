"""
Simple static file server for local development.

Usage:
    python serve.py          # serves on port 8000
    PORT=9000 python serve.py
"""

import http.server
import os
from pathlib import Path

ROOT = Path(__file__).parent
PORT = int(os.environ.get("PORT", 8000))


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence per-request noise; remove to see access logs


os.chdir(ROOT)
print(f"Serving {ROOT} on http://localhost:{PORT}")
with http.server.HTTPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
