"""
Message generators for synthetic or customized data feeds.
"""

from abc import ABC, abstractmethod
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


class BaseMessageGenerator(ABC):
    """Abstract base class for generating Kafka messages."""

    @abstractmethod
    def generate(self) -> Tuple[Optional[Any], Any, Optional[List[Tuple[str, bytes]]]]:
        """
        Generate a message record.

        Returns:
            Tuple of (key, value, headers)
        """
        pass


class ProductDataGenerator(BaseMessageGenerator):
    """
    Generates synthetic Product/User events matching the standard schema:
    {
        "registertime": 1509912177542,
        "userid": "User_4",
        "regionid": "Region_6",
        "gender": "FEMALE"
    }
    """

    GENDERS = ["MALE", "FEMALE", "OTHER"]

    def __init__(self, num_users: int = 10, num_regions: int = 9):
        self.num_users = num_users
        self.num_regions = num_regions

    def generate(self) -> Tuple[str, Dict[str, Any], Optional[List[Tuple[str, bytes]]]]:
        user_num = random.randint(1, self.num_users)
        region_num = random.randint(1, self.num_regions)
        gender = random.choice(self.GENDERS)
        user_id = f"User_{user_num}"
        region_id = f"Region_{region_num}"

        payload = {
            "registertime": int(time.time() * 1000),
            "userid": user_id,
            "regionid": region_id,
            "gender": gender,
        }

        headers = [
            ("producer", b"kafka-k8s-producer"),
            ("version", b"1.0"),
        ]

        return user_id, payload, headers


class CallableMessageGenerator(BaseMessageGenerator):
    """Generates messages by invoking a custom callable."""

    def __init__(self, callback: Callable[[], Tuple[Optional[Any], Any]]):
        self._callback = callback

    def generate(self) -> Tuple[Optional[Any], Any, Optional[List[Tuple[str, bytes]]]]:
        result = self._callback()
        if len(result) == 2:
            return result[0], result[1], None
        return result[0], result[1], result[2]
