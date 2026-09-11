"""
Serialization module for Kafka producers.
Provides safe encoding for dictionaries, lists, strings, and raw bytes.
"""

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SafeSerializer:
    """
    Safely serializes message keys and payloads into bytes.
    - None -> None
    - dict/list -> UTF-8 encoded JSON bytes
    - str -> UTF-8 encoded bytes
    - bytes -> raw bytes unchanged
    - other -> stringified UTF-8 bytes
    """

    def serialize(self, topic: Optional[str], value: Any) -> Optional[bytes]:
        return self._encode(value)

    def __call__(self, value: Any) -> Optional[bytes]:
        return self._encode(value)

    def _encode(self, value: Any) -> Optional[bytes]:
        if value is None:
            return None

        if isinstance(value, bytes):
            return value

        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value).encode("utf-8")
            except (TypeError, ValueError) as e:
                logger.warning(f"JSON serialization error: {e}. Falling back to string.")
                return str(value).encode("utf-8")

        if isinstance(value, str):
            return value.encode("utf-8")

        return str(value).encode("utf-8")
