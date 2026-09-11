"""
Kubernetes Health Server exposing /healthz and /readyz endpoints.
Implemented using Python's standard library http.server on a background daemon thread.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class HealthState:
    """Thread-safe state store for health and readiness."""

    def __init__(self):
        self._lock = threading.Lock()
        self.is_alive = True
        self.is_ready = False
        self.last_poll_time = time.time()
        self.messages_consumed = 0
        self.assigned_partitions = 0
        self.topics = []

    def mark_ready(self, ready: bool = True) -> None:
        with self._lock:
            self.is_ready = ready

    def mark_alive(self, alive: bool = True) -> None:
        with self._lock:
            self.is_alive = alive

    def update_poll(self, message_count: int = 0) -> None:
        with self._lock:
            self.last_poll_time = time.time()
            self.messages_consumed += message_count

    def update_partitions(self, count: int) -> None:
        with self._lock:
            self.assigned_partitions = count

    def get_status(self) -> dict:
        with self._lock:
            return {
                "alive": self.is_alive,
                "ready": self.is_ready,
                "uptime_seconds": round(time.time() - self.last_poll_time, 2),
                "messages_consumed": self.messages_consumed,
                "assigned_partitions": self.assigned_partitions,
                "topics": list(self.topics),
            }


class HealthRequestHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for health and readiness probes."""

    state: HealthState = None  # Injected by HealthServer

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._handle_healthz()
        elif self.path == "/readyz":
            self._handle_readyz()
        elif self.path in ("/status", "/metrics"):
            self._handle_status()
        else:
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error": "Not Found"}')

    def _handle_healthz(self) -> None:
        if self.state and self.state.is_alive:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "UP"}')
        else:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "DOWN"}')

    def _handle_readyz(self) -> None:
        if self.state and self.state.is_ready:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "READY"}')
        else:
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "NOT_READY"}')

    def _handle_status(self) -> None:
        data = self.state.get_status() if self.state else {}
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:
        # Suppress standard logging for frequent probe requests to keep logs clean
        logger.debug("HealthServer probe: " + (format % args))


class HealthServer:
    """Embedded HTTP server for Kubernetes probes running in a background thread."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        self.host = host
        self.port = port
        self.state = HealthState()
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        handler_class = HealthRequestHandler
        handler_class.state = self.state

        try:
            self._server = HTTPServer((self.host, self.port), handler_class)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            logger.info(f"Kubernetes health check server started on {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to start health server on {self.host}:{self.port}: {e}")

    def stop(self) -> None:
        self.state.mark_ready(False)
        self.state.mark_alive(False)
        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception as e:
                logger.debug(f"Error closing health server: {e}")
            logger.info("Health server stopped.")
