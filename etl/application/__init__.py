from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class EventPublisher(ABC):
    """
    Port (outbound interface) for publishing domain events to a message broker.

    Defined in the application layer -- infrastructure (KafkaEventPublisher)
    implements it. The domain and application layers depend on this interface,
    not on any Kafka-specific types, so the broker can be swapped without
    touching domain or application code.
    """

    @abstractmethod
    def publish_batch(self, topic: str, messages: list[dict[str, Any]]) -> None:
        """Publish a list of message dicts to the given topic."""

    @abstractmethod
    def flush(self, timeout: float = 30.0) -> int:
        """Flush buffered messages and return the count of undelivered messages."""


__all__ = ["EventPublisher"]
