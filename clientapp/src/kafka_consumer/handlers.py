"""
Message handler architecture for processing consumed Kafka messages.
Allows plugging in custom business logic, sinks, or logging handlers.
"""

from abc import ABC, abstractmethod
import json
import logging
from typing import Any, Callable, List

logger = logging.getLogger(__name__)


class BaseMessageHandler(ABC):
    """Abstract base class for Kafka message handlers."""

    @abstractmethod
    def handle(self, record: Any) -> None:
        """
        Process a consumed Kafka record.

        Args:
            record: ConsumerRecord object with attributes
                    (topic, partition, offset, key, value, headers, timestamp)
        """
        pass


class LoggingMessageHandler(BaseMessageHandler):
    """
    Default message handler that logs message details in the format
    expected by the application.
    """

    def __init__(self, use_logger: bool = False):
        self.use_logger = use_logger

    def handle(self, record: Any) -> None:
        key_str = record.key.decode("utf-8") if isinstance(record.key, bytes) else str(record.key)
        
        # Pretty print if JSON dict/list
        if isinstance(record.value, (dict, list)):
            payload_str = json.dumps(record.value, indent=2)
        else:
            payload_str = str(record.value)

        header_line = f"[Topic: {record.topic} | Partition {record.partition} | Offset {record.offset} | Key: {key_str}]"
        payload_line = f"Payload: {payload_str}"
        divider = "-" * 50

        if self.use_logger:
            logger.info(header_line)
            logger.info(payload_line)
        else:
            print(header_line)
            print(payload_line)
            print(divider, flush=True)


class CallableMessageHandler(BaseMessageHandler):
    """Wraps any callable function as a message handler."""

    def __init__(self, callback: Callable[[Any], None]):
        self._callback = callback

    def handle(self, record: Any) -> None:
        self._callback(record)


class CompositeMessageHandler(BaseMessageHandler):
    """Dispatches each message sequentially to a list of handlers."""

    def __init__(self, handlers: List[BaseMessageHandler]):
        self._handlers = handlers

    def add_handler(self, handler: BaseMessageHandler) -> None:
        self._handlers.append(handler)

    def handle(self, record: Any) -> None:
        for handler in self._handlers:
            try:
                handler.handle(record)
            except Exception as e:
                logger.error(f"Error in handler {handler.__class__.__name__}: {e}", exc_info=True)
