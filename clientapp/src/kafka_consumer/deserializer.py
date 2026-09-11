"""
Deserializer module for Kafka messages.
Provides safe, resilient payload decoding for UTF-8 text and JSON.
"""

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SafeDeserializer:
    """
    Safely deserializes message payloads.
    - None -> None
    - Valid JSON string -> parsed dict/list/primitive
    - UTF-8 text -> string
    - Binary data -> string representation of bytes
    Compatible with both kafka-python Deserializer interface and direct callable.
    """

    def deserialize(self, topic: Optional[str], headers: Any, raw_bytes: Optional[bytes]) -> Any:
        """Standard kafka Deserializer interface."""
        return self._decode(raw_bytes)

    def __call__(self, raw_bytes: Optional[bytes]) -> Any:
        """Callable interface for value_deserializer=SafeDeserializer()."""
        return self._decode(raw_bytes)

    def _decode(self, raw_bytes: Optional[bytes]) -> Any:
        if raw_bytes is None:
            return None

        try:
            text = raw_bytes.decode("utf-8")
            try:
                return json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return text
        except UnicodeDecodeError:
            logger.debug("Payload is non-UTF8 binary data; converting to string representation.")
            return str(raw_bytes)
        except Exception as ex:
            logger.warning(f"Unexpected error while deserializing payload: {ex}")
            return str(raw_bytes)
