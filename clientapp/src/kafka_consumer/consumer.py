"""
Main Kafka Consumer implementation for Kubernetes environments.
Features:
- Configurable via ConsumerConfig or environment variables
- Resilient startup with exponential backoff retries
- Graceful shutdown handling (SIGINT / SIGTERM)
- Integrated Kubernetes health probes (readiness / liveness)
- Safe deserialization (JSON, UTF-8 text, raw fallback)
- Pluggable message handlers
"""

import logging
import signal
import sys
import time
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from .config import ConsumerConfig
from .deserializer import SafeDeserializer
from .handlers import BaseMessageHandler, LoggingMessageHandler
from .health import HealthServer

logger = logging.getLogger(__name__)


class GenericKafkaConsumer:
    """Production-ready Kafka Consumer for Kubernetes."""

    def __init__(
        self,
        config: Optional[ConsumerConfig] = None,
        handler: Optional[BaseMessageHandler] = None,
        deserializer: Optional[SafeDeserializer] = None,
    ):
        self.config = config or ConsumerConfig.from_env()
        self.config.validate()

        self.handler = handler or LoggingMessageHandler()
        self.deserializer = deserializer or SafeDeserializer()

        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._health_server: Optional[HealthServer] = None

        if self.config.health_enabled:
            self._health_server = HealthServer(
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

    def _create_consumer_with_retry(self, max_retries: int = 5, initial_delay: float = 2.0) -> KafkaConsumer:
        """Create KafkaConsumer instance with exponential backoff retry."""
        kafka_params = self.config.to_kafka_params()
        kafka_params["value_deserializer"] = self.deserializer

        delay = initial_delay
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Connecting to Kafka bootstrap servers: {self.config.bootstrap_servers} "
                    f"(attempt {attempt}/{max_retries}, protocol: {self.config.security_protocol})..."
                )
                consumer = KafkaConsumer(
                    *self.config.topics,
                    **kafka_params,
                )
                logger.info(
                    f"Successfully connected to Kafka. Subscribed to topics: {self.config.topics} "
                    f"with Group ID: '{self.config.group_id}'"
                )
                return consumer
            except (KafkaError, Exception) as e:
                logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    logger.error(f"Exceeded max connection retries ({max_retries}). Aborting.")
                    raise
                logger.info(f"Retrying connection in {delay:.1f} seconds...")
                time.sleep(delay)
                delay *= 2.0

        raise RuntimeError("Failed to establish Kafka connection.")

    def start(self) -> None:
        """Start the Kafka consumer loop."""
        self._setup_signals()

        # Start Kubernetes health server
        if self._health_server:
            self._health_server.start()
            self._health_server.state.topics = self.config.topics

        # Connect to Kafka
        self._consumer = self._create_consumer_with_retry()
        self._running = True

        if self._health_server:
            self._health_server.state.mark_ready(True)

        logger.info("==================================================")
        logger.info(f"Kafka Consumer Started Successfully")
        logger.info(f"Topics: {self.config.topics}")
        logger.info(f"Bootstrap Servers: {self.config.bootstrap_servers}")
        logger.info(f"Consumer Group: {self.config.group_id}")
        logger.info(f"Security Protocol: {self.config.security_protocol}")
        if self._health_server:
            logger.info(f"Health Probes: http://{self.config.health_host}:{self.config.health_port}/healthz and /readyz")
        logger.info("Listening for messages... Press Ctrl+C or send SIGTERM to exit.")
        logger.info("==================================================\n")

        try:
            while self._running:
                # Use poll to prevent blocking indefinitely, allowing clean shutdown on signal
                msg_dict = self._consumer.poll(timeout_ms=1000, max_records=self.config.max_poll_records)

                if not msg_dict:
                    continue

                total_messages = 0
                for topic_partition, records in msg_dict.items():
                    for record in records:
                        total_messages += 1
                        try:
                            self.handler.handle(record)
                        except Exception as e:
                            logger.error(
                                f"Error processing message from {record.topic} "
                                f"[partition {record.partition}, offset {record.offset}]: {e}",
                                exc_info=True,
                            )

                if self._health_server:
                    self._health_server.state.update_poll(message_count=total_messages)
                    if hasattr(self._consumer, "assignment"):
                        self._health_server.state.update_partitions(len(self._consumer.assignment()))

        except Exception as e:
            if self._running:
                logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
                raise
        finally:
            self._cleanup()

    def stop(self) -> None:
        """Signal consumer to stop gracefully."""
        self._running = False
        if self._health_server:
            # Mark pod not ready immediately during Kubernetes termination grace period
            self._health_server.state.mark_ready(False)

    def _cleanup(self) -> None:
        """Clean up consumer connections and health server."""
        logger.info("Cleaning up Kafka consumer resources...")
        if self._consumer:
            try:
                self._consumer.close(autocommit=self.config.enable_auto_commit)
                logger.info("Kafka consumer closed successfully.")
            except Exception as e:
                logger.warning(f"Error while closing Kafka consumer: {e}")

        if self._health_server:
            self._health_server.stop()

        logger.info("Consumer shutdown complete. Bye!")
