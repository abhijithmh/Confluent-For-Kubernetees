"""
Command-line entrypoint for running the Generic Kafka Producer.
"""

import argparse
import logging
import sys

from .config import ProducerConfig
from .generators import ProductDataGenerator
from .producer import GenericKafkaProducer


def setup_logging(log_level: str) -> None:
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Production Kafka Producer for Kubernetes and Confluent Platform."
    )
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        help="Kafka bootstrap servers (comma-separated). Overrides KAFKA_BOOTSTRAP_SERVERS env var.",
    )
    parser.add_argument(
        "--topic",
        "-t",
        help="Target topic name. Overrides KAFKA_TOPIC env var.",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=float,
        help="Delay in seconds between sends (default: 1.0).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Messages per batch/interval (default: 1).",
    )
    parser.add_argument(
        "--max-messages",
        "-n",
        type=int,
        help="Stop after sending N messages (default: infinite).",
    )
    parser.add_argument(
        "--acks",
        choices=["all", "0", "1"],
        help="Acknowledgment level (default: all).",
    )
    parser.add_argument(
        "--compression",
        choices=["lz4", "gzip", "snappy", "zstd", "none"],
        help="Compression codec (default: None).",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--no-health-server",
        action="store_true",
        help="Disable Kubernetes health check server.",
    )

    args = parser.parse_args()

    config = ProducerConfig.from_env()

    if args.bootstrap_servers:
        config.bootstrap_servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
    if args.topic:
        config.topic = args.topic
    if args.interval is not None:
        config.produce_interval_seconds = args.interval
    if args.batch_size is not None:
        config.produce_batch_size = args.batch_size
    if args.max_messages is not None:
        config.max_messages = args.max_messages
    if args.acks:
        config.acks = args.acks
    if args.compression:
        config.compression_type = None if args.compression == "none" else args.compression
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

    producer = GenericKafkaProducer(
        config=config,
        generator=ProductDataGenerator(),
    )

    try:
        producer.start()
    except Exception as e:
        logging.error(f"Fatal error running producer: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
