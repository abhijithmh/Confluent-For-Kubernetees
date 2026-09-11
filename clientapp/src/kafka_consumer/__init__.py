"""
Kafka Consumer Package for Confluent on Kubernetes.
"""

from .config import ConsumerConfig
from .consumer import GenericKafkaConsumer
from .deserializer import SafeDeserializer
from .handlers import (
    BaseMessageHandler,
    CallableMessageHandler,
    CompositeMessageHandler,
    LoggingMessageHandler,
)
from .health import HealthServer, HealthState

__all__ = [
    "ConsumerConfig",
    "GenericKafkaConsumer",
    "SafeDeserializer",
    "BaseMessageHandler",
    "LoggingMessageHandler",
    "CallableMessageHandler",
    "CompositeMessageHandler",
    "HealthServer",
    "HealthState",
]
