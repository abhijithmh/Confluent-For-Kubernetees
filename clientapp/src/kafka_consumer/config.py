"""
Configuration module for Generic Kafka Consumer.
Follows the 12-factor app methodology by reading configuration from environment variables.
"""

from dataclasses import dataclass, field
import logging
import os
from typing import Any, Dict, List, Optional


def _str_to_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "on")


@dataclass
class ConsumerConfig:
    """Kafka Consumer Configuration loaded from environment variables."""

    # Kafka Connection
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    topics: List[str] = field(default_factory=lambda: ["Product"])
    group_id: str = "my-python-consumer-group"

    # Security & Authentication
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None

    # Consumer Offsets & Timing
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 10000
    request_timeout_ms: int = 30000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000

    # Kubernetes Health Server
    health_enabled: bool = True
    health_host: str = "0.0.0.0"
    health_port: int = 8080

    # Logging
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ConsumerConfig":
        """Load configuration from environment variables with backwards-compatible fallbacks."""

        # Bootstrap servers: comma-separated or single
        raw_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
        servers = [s.strip() for s in raw_servers.split(",") if s.strip()]

        # Topics: comma-separated or single
        raw_topics = os.getenv("KAFKA_TOPICS") or os.getenv("KAFKA_TOPIC") or os.getenv("TOPIC_NAME", "Product")
        topics = [t.strip() for t in raw_topics.split(",") if t.strip()]

        # Consumer Group
        group_id = os.getenv("KAFKA_GROUP_ID") or os.getenv("GROUP_ID", "my-python-consumer-group")

        # Credentials
        username = os.getenv("KAFKA_USERNAME") or os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_PASSWORD") or os.getenv("KAFKA_SASL_PASSWORD")

        # Security protocol: default to SASL_PLAINTEXT if credentials are provided and protocol not specified
        default_protocol = "SASL_PLAINTEXT" if username and password else "PLAINTEXT"
        security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or os.getenv("SECURITY_PROTOCOL", default_protocol)
        sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM") or os.getenv("SASL_MECHANISM", "PLAIN")

        # SSL Certificates
        ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
        ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
        ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")

        # Offsets and timing
        auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        enable_auto_commit = _str_to_bool(os.getenv("KAFKA_ENABLE_AUTO_COMMIT"), default=True)
        auto_commit_interval_ms = int(os.getenv("KAFKA_AUTO_COMMIT_INTERVAL_MS", "5000"))
        session_timeout_ms = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "10000"))
        request_timeout_ms = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))
        max_poll_records = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500"))
        max_poll_interval_ms = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))

        # Health Server
        health_enabled = _str_to_bool(os.getenv("HEALTH_ENABLED"), default=True)
        health_host = os.getenv("HEALTH_HOST", "0.0.0.0")
        health_port = int(os.getenv("HEALTH_PORT", "8080"))

        # Logging
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        return cls(
            bootstrap_servers=servers,
            topics=topics,
            group_id=group_id,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            session_timeout_ms=session_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            max_poll_records=max_poll_records,
            max_poll_interval_ms=max_poll_interval_ms,
            health_enabled=health_enabled,
            health_host=health_host,
            health_port=health_port,
            log_level=log_level,
        )

    def validate(self) -> None:
        """Validate configuration parameters."""
        if not self.bootstrap_servers:
            raise ValueError("At least one bootstrap server must be configured.")
        if not self.topics:
            raise ValueError("At least one topic must be specified.")
        if not self.group_id:
            raise ValueError("Consumer group_id cannot be empty.")

        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            if not self.sasl_plain_username or not self.sasl_plain_password:
                raise ValueError(
                    f"Credentials KAFKA_USERNAME and KAFKA_PASSWORD are required for {self.security_protocol}."
                )

        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_cafile and not os.path.exists(self.ssl_cafile):
                raise FileNotFoundError(f"SSL CA file not found: {self.ssl_cafile}")

        # Kafka requires request_timeout_ms > session_timeout_ms
        if self.request_timeout_ms <= self.session_timeout_ms:
            self.request_timeout_ms = self.session_timeout_ms + 10000

    def to_kafka_params(self) -> Dict[str, Any]:
        """Convert configuration to parameters dictionary for KafkaConsumer."""
        params: Dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "auto_commit_interval_ms": self.auto_commit_interval_ms,
            "session_timeout_ms": self.session_timeout_ms,
            "request_timeout_ms": self.request_timeout_ms,
            "max_poll_records": self.max_poll_records,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "security_protocol": self.security_protocol,
        }

        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            params["sasl_mechanism"] = self.sasl_mechanism
            params["sasl_plain_username"] = self.sasl_plain_username
            params["sasl_plain_password"] = self.sasl_plain_password

        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_cafile:
                params["ssl_cafile"] = self.ssl_cafile
            if self.ssl_certfile:
                params["ssl_certfile"] = self.ssl_certfile
            if self.ssl_keyfile:
                params["ssl_keyfile"] = self.ssl_keyfile

        return params
