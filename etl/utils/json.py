from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID


class ETLJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles types common in the ETL pipeline.

    Extends the standard encoder to support:
      - datetime / date  : ISO 8601 string
      - Decimal          : float (acceptable precision for fare amounts)
      - UUID             : str
      - bytes            : hex string (for binary Kafka keys)
      - objects with to_dict() : delegate to that method

    Used by dumps() and by Kafka serialiser when publishing trip events.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        return super().default(obj)


def dumps(obj: Any, **kwargs: Any) -> str:
    """
    Serialise obj to a JSON string using ETLJSONEncoder.

    Passes through any additional kwargs to json.dumps() so callers
    can set indent=2 for human-readable output (e.g. rejected files).
    """
    return json.dumps(obj, cls=ETLJSONEncoder, **kwargs)


def loads(s: str | bytes) -> Any:
    """
    Deserialise a JSON string. Thin wrapper kept for import symmetry
    and so a custom object_hook can be added here in future.
    """
    return json.loads(s)


def dumps_lines(records: list[dict[str, Any]]) -> str:
    """
    Serialise a list of dicts to newline-delimited JSON (JSON Lines).

    Used when writing rejected records to data/rejected/ files.
    Each line is a self-contained JSON object for easy grep and replay.
    """
    return "\n".join(dumps(record) for record in records)
