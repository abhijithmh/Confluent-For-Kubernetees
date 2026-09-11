"""
Command-line entrypoint for running the Generic Kafka Consumer.
"""

import argparse
import logging
import sys

from .config import ConsumerConfig
from .consumer import GenericKafkaConsumer
from .handlers import LoggingMessageHandler


def setup_logging(log_level: str) -> None:
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production Kafka Consumer for Kubernetes and Confluent Platform."
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        help="Kafka bootstrap servers (comma-separated). Overrides KAFKA_BOOTSTRAP_SERVERS env var.",
    )
    parser.add_argument(
        "--topics",
        "-t",
        help="Topics to subscribe to (comma-separated). Overrides KAFKA_TOPICS env var.",
    )
    parser.add_argument(
        "--group-id",
        "-g",
        help="Consumer group ID. Overrides KAFKA_GROUP_ID env var.",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--no-health-server",
        action="store_true",
        help="Disable Kubernetes health check server",
    )

    args = parser.parse_args()

    config = ConsumerConfig.from_env()

    # Apply CLI overrides if provided
    if args.bootstrap_servers:
        config.bootstrap_servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
    if args.topics:
        config.topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    if args.group_id:
        config.group_id = args.group_id
    if args.log_level:
        config.log_level = args.log_level
    if args.no_health_server:
        config.health_enabled = False

    setup_logging(config.log_level)

    try:
        config.validate()
    except Exception as e:
        logging.error(f"Configuration error: {e}")
        sys.exit(1)

    # Use LoggingMessageHandler by default
    handler = LoggingMessageHandler(use_logger=False)

    consumer = GenericKafkaConsumer(
        config=config,
        handler=handler,
    )

    try:
        consumer.start()
    except Exception as e:
        logging.error(f"Fatal error starting consumer: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
