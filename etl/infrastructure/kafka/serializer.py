from __future__ import annotations

import logging
from typing import Any

from etl.utils.json import dumps, loads

logger = logging.getLogger(__name__)


class KafkaSerializer:
    """
    Handles JSON serialisation and deserialisation for Kafka messages.

    Uses the shared JSON utilities to encode types such as:
    - datetime : ISO 8601 string
    - Decimal  : float
    - UUID     : string
    - bytes    : hex string

    This ensures a consistent wire format between producers and consumers.
    """

    def serialize(self, message: dict[str, Any]) -> bytes:
        """
        Serialise a dict to UTF-8 encoded JSON bytes.

        Raises ValueError if the message contains types that cannot
        be handled by ETLJSONEncoder.
        """
        try:
            return dumps(message).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Cannot serialise Kafka message: {exc}. Message keys: {list(message.keys())}"
            ) from exc

    def deserialize(self, data: bytes | None) -> dict[str, Any] | None:
        """
        Deserialise UTF-8 JSON bytes to a dict.

        Returns None if data is None or empty, which can happen with
        Kafka tombstone messages (delete markers).
        """
        if not data:
            return None
        try:
            return loads(data.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            logger.error(
                "Failed to deserialise Kafka message: %s",
                exc,
                extra={"raw_length": len(data)},
            )
            return None

    def serialize_batch(self, messages: list[dict[str, Any]]) -> list[bytes]:
        """Serialise a list of dicts, returning a list of byte payloads."""
        return [self.serialize(m) for m in messages]
