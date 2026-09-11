"""
Kubernetes Health Server for Kafka Producer exposing /healthz, /readyz, and /status.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class ProducerHealthState:
    """Thread-safe state store for producer health metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self.is_alive = True
        self.is_ready = False
        self.messages_sent = 0
        self.messages_acked = 0
        self.send_errors = 0
        self.last_send_time = time.time()
        self.topic = ""

    def mark_ready(self, ready: bool = True) -> None:
        with self._lock:
            self.is_ready = ready

    def mark_alive(self, alive: bool = True) -> None:
        with self._lock:
            self.is_alive = alive

    def record_send(self, count: int = 1) -> None:
        with self._lock:
            self.messages_sent += count
            self.last_send_time = time.time()

    def record_ack(self, count: int = 1) -> None:
        with self._lock:
            self.messages_acked += count

    def record_error(self) -> None:
        with self._lock:
            self.send_errors += 1

    def get_status(self) -> dict:
        with self._lock:
            return {
                "alive": self.is_alive,
                "ready": self.is_ready,
                "topic": self.topic,
                "messages_sent": self.messages_sent,
                "messages_acked": self.messages_acked,
                "send_errors": self.send_errors,
                "uptime_seconds": round(time.time() - self.last_send_time, 2),
            }


class ProducerHealthRequestHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for producer health probes."""

    state: ProducerHealthState = None

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
        logger.debug("Producer HealthServer probe: " + (format % args))


class ProducerHealthServer:
    """Embedded HTTP server for Kubernetes producer health checks."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8081):
        self.host = host
        self.port = port
        self.state = ProducerHealthState()
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        handler_class = ProducerHealthRequestHandler
        handler_class.state = self.state

        try:
            self._server = HTTPServer((self.host, self.port), handler_class)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            logger.info(f"Producer health server listening on {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to start producer health server on {self.host}:{self.port}: {e}")

    def stop(self) -> None:
        self.state.mark_ready(False)
        self.state.mark_alive(False)
        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception as e:
                logger.debug(f"Error stopping health server: {e}")
            logger.info("Producer health server stopped.")
