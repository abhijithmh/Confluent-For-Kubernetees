"""
Configuration module for Generic Kafka Producer.
Follows the 12-factor app methodology by loading configuration from environment variables.
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
class ProducerConfig:
    """Kafka Producer Configuration loaded from environment variables."""

    # Kafka Connection
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    topic: str = "Product"

    # Security & Authentication
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    ssl_cafile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None

    # Producer Reliability & Performance
    acks: str = "all"
    retries: int = 5
    retry_backoff_ms: int = 500
    compression_type: Optional[str] = None  # None, 'gzip', 'snappy', 'lz4', 'zstd'
    batch_size: int = 16384
    linger_ms: int = 10
    request_timeout_ms: int = 30000

    # Production Rate & Limits
    produce_interval_seconds: float = 1.0  # Delay between sends (0 for continuous)
    produce_batch_size: int = 1            # Messages to send per interval
    max_messages: int = -1                 # -1 for infinite

    # Kubernetes Health Server
    health_enabled: bool = True
    health_host: str = "0.0.0.0"
    health_port: int = 8081

    # Logging
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        """Load configuration from environment variables with sensible defaults."""
        raw_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
        servers = [s.strip() for s in raw_servers.split(",") if s.strip()]

        topic = os.getenv("KAFKA_TOPIC") or os.getenv("TOPIC_NAME", "Product")

        # Credentials
        username = os.getenv("KAFKA_USERNAME") or os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_PASSWORD") or os.getenv("KAFKA_SASL_PASSWORD")

        # Security protocol
        default_protocol = "SASL_PLAINTEXT" if username and password else "PLAINTEXT"
        security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL") or os.getenv("SECURITY_PROTOCOL", default_protocol)
        sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM") or os.getenv("SASL_MECHANISM", "PLAIN")

        # SSL Certificates
        ssl_cafile = os.getenv("KAFKA_SSL_CAFILE")
        ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE")
        ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE")

        # Reliability
        acks = os.getenv("KAFKA_ACKS", "all")
        retries = int(os.getenv("KAFKA_RETRIES", "5"))
        retry_backoff_ms = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "500"))
        compression_type = os.getenv("KAFKA_COMPRESSION_TYPE")
        if compression_type and compression_type.lower() in ("none", ""):
            compression_type = None

        batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "16384"))
        linger_ms = int(os.getenv("KAFKA_LINGER_MS", "10"))
        request_timeout_ms = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))

        # Interval and rate
        produce_interval_seconds = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "1.0"))
        produce_batch_size = int(os.getenv("PRODUCE_BATCH_SIZE", "1"))
        max_messages = int(os.getenv("MAX_MESSAGES", "-1"))

        # Health server
        health_enabled = _str_to_bool(os.getenv("HEALTH_ENABLED"), default=True)
        health_host = os.getenv("HEALTH_HOST", "0.0.0.0")
        health_port = int(os.getenv("HEALTH_PORT", "8081"))

        # Logging
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        return cls(
            bootstrap_servers=servers,
            topic=topic,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=username,
            sasl_plain_password=password,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            acks=acks,
            retries=retries,
            retry_backoff_ms=retry_backoff_ms,
            compression_type=compression_type,
            batch_size=batch_size,
            linger_ms=linger_ms,
            request_timeout_ms=request_timeout_ms,
            produce_interval_seconds=produce_interval_seconds,
            produce_batch_size=produce_batch_size,
            max_messages=max_messages,
            health_enabled=health_enabled,
            health_host=health_host,
            health_port=health_port,
            log_level=log_level,
        )

    def validate(self) -> None:
        """Validate producer configuration."""
        if not self.bootstrap_servers:
            raise ValueError("At least one bootstrap server must be configured.")
        if not self.topic:
            raise ValueError("Target topic cannot be empty.")

        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            if not self.sasl_plain_username or not self.sasl_plain_password:
                raise ValueError(
                    f"Credentials KAFKA_USERNAME and KAFKA_PASSWORD are required for {self.security_protocol}."
                )

        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_cafile and not os.path.exists(self.ssl_cafile):
                raise FileNotFoundError(f"SSL CA file not found: {self.ssl_cafile}")

    def to_kafka_params(self) -> Dict[str, Any]:
        """Convert configuration to parameters dictionary for KafkaProducer."""
        # Normalize acks (all or string/int)
        acks_val: Any = self.acks
        if acks_val.isdigit():
            acks_val = int(acks_val)

        params: Dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "acks": acks_val,
            "retries": self.retries,
            "retry_backoff_ms": self.retry_backoff_ms,
            "batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
            "request_timeout_ms": self.request_timeout_ms,
            "security_protocol": self.security_protocol,
        }

        if self.compression_type:
            params["compression_type"] = self.compression_type

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
