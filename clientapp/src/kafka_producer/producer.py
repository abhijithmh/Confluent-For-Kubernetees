"""
Main Generic Kafka Producer implementation for Kubernetes environments.
Features:
- 12-factor configuration via ProducerConfig or environment variables
- Resilient startup with exponential backoff retries
- Asynchronous sending with delivery callbacks
- Pluggable message generators (default: ProductDataGenerator)
- Kubernetes health probes (/healthz and /readyz)
- Graceful shutdown handling (SIGINT / SIGTERM) with buffer flushing
"""

import logging
import signal
import sys
import time
from typing import Any, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .config import ProducerConfig
from .generators import BaseMessageGenerator, ProductDataGenerator
from .health import ProducerHealthServer
from .serializer import SafeSerializer

logger = logging.getLogger(__name__)


class GenericKafkaProducer:
    """Production-ready Kafka Producer for Kubernetes."""

    def __init__(
        self,
        config: Optional[ProducerConfig] = None,
        generator: Optional[BaseMessageGenerator] = None,
        key_serializer: Optional[SafeSerializer] = None,
        value_serializer: Optional[SafeSerializer] = None,
    ):
        self.config = config or ProducerConfig.from_env()
        self.config.validate()

        self.generator = generator or ProductDataGenerator()
        self.key_serializer = key_serializer or SafeSerializer()
        self.value_serializer = value_serializer or SafeSerializer()

        self._producer: Optional[KafkaProducer] = None
        self._running = False
        self._health_server: Optional[ProducerHealthServer] = None

        if self.config.health_enabled:
            self._health_server = ProducerHealthServer(
                host=self.config.health_host,
                port=self.config.health_port,
            )

    def _setup_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        def _signal_handler(sig, frame):
            sig_name = signal.Signals(sig).name
            logger.info(f"Received signal {sig_name}. Initiating graceful shutdown...")
            self.stop()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    def _create_producer_with_retry(self, max_retries: int = 5, initial_delay: float = 2.0) -> KafkaProducer:
        """Create KafkaProducer instance with exponential backoff retry."""
        kafka_params = self.config.to_kafka_params()
        kafka_params["key_serializer"] = self.key_serializer
        kafka_params["value_serializer"] = self.value_serializer

        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Connecting to Kafka bootstrap servers: {self.config.bootstrap_servers} "
                    f"(attempt {attempt}/{max_retries}, protocol: {self.config.security_protocol})..."
                )
                producer = KafkaProducer(**kafka_params)
                logger.info(
                    f"Successfully connected to Kafka producer. Target topic: '{self.config.topic}', "
                    f"acks={self.config.acks}"
                )
                return producer
            except (KafkaError, Exception) as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    logger.error(f"Exceeded max connection retries ({max_retries}). Aborting.")
                    raise
                logger.info(f"Retrying connection in {delay:.1f} seconds...")
                time.sleep(delay)
                delay *= 2.0

        raise RuntimeError("Failed to establish Kafka producer connection.")

    def _on_send_success(self, record_metadata: Any) -> None:
        """Callback invoked when a message is successfully delivered to broker."""
        if self._health_server:
            self._health_server.state.record_ack()

        logger.info(
            f"Delivered -> Topic: '{record_metadata.topic}' | Partition: {record_metadata.partition} | "
            f"Offset: {record_metadata.offset}"
        )

    def _on_send_error(self, exc: Exception) -> None:
        """Callback invoked when message delivery fails."""
        if self._health_server:
            self._health_server.state.record_error()

        logger.error(f"Failed to deliver message: {exc}", exc_info=True)

    def send_one(self) -> Any:
        """Generate and produce a single record."""
        if not self._producer:
            raise RuntimeError("Producer not started.")

        key, value, headers = self.generator.generate()

        future = self._producer.send(
            topic=self.config.topic,
            key=key,
            value=value,
            headers=headers,
        )
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)

        if self._health_server:
            self._health_server.state.record_send()

        return future

    def start(self) -> None:
        """Start the producer loop."""
        self._setup_signals()

        if self._health_server:
            self._health_server.state.topic = self.config.topic
            self._health_server.start()

        self._producer = self._create_producer_with_retry()
        self._running = True

        if self._health_server:
            self._health_server.state.mark_ready(True)

        logger.info("==================================================")
        logger.info("Kafka Producer Started Successfully")
        logger.info(f"Target Topic: {self.config.topic}")
        logger.info(f"Bootstrap Servers: {self.config.bootstrap_servers}")
        logger.info(f"Interval: {self.config.produce_interval_seconds}s | Acks: {self.config.acks}")
        if self._health_server:
            logger.info(f"Health Probes: http://{self.config.health_host}:{self.config.health_port}/healthz and /readyz")
        logger.info("Streaming messages... Press Ctrl+C or send SIGTERM to stop.")
        logger.info("==================================================\n")

        sent_count = 0
        try:
            while self._running:
                for _ in range(self.config.produce_batch_size):
                    self.send_one()
                    sent_count += 1

                    if self.config.max_messages > 0 and sent_count >= self.config.max_messages:
                        logger.info(f"Reached max_messages limit ({self.config.max_messages}). Stopping producer.")
                        self.stop()
                        break

                if self.config.produce_interval_seconds > 0 and self._running:
                    time.sleep(self.config.produce_interval_seconds)

            # Flush any remaining in-flight records
            logger.info("Flushing final producer buffer...")
            self._producer.flush(timeout=10)

        except Exception as e:
            if self._running:
                logger.error(f"Unexpected error in producer loop: {e}", exc_info=True)
                raise
        finally:
            self._cleanup()

    def stop(self) -> None:
        """Signal producer to stop."""
        self._running = False
        if self._health_server:
            self._health_server.state.mark_ready(False)

    def _cleanup(self) -> None:
        """Clean up producer resources."""
        logger.info("Closing Kafka producer...")
        if self._producer:
            try:
                self._producer.flush(timeout=5)
                self._producer.close(timeout=5)
                logger.info("Kafka producer closed cleanly.")
            except Exception as e:
                logger.warning(f"Error while closing producer: {e}")

        if self._health_server:
            self._health_server.stop()

        logger.info("Producer shutdown complete. Bye!")
