from __future__ import annotations

import hashlib
from datetime import datetime

from etl.domain.trip.models import Trip


def generate_trip_id(
    vendor_id: str,
    pickup_datetime: datetime,
    dropoff_datetime: datetime,
    pickup_location_id: int,
) -> str:
    """
    Generate a deterministic SHA-256 trip ID from the natural key fields.

    The same combination of vendor, pickup time, dropoff time, and pickup
    zone always produces the same hex digest. This makes trip_id stable
    across Kafka replays so ClickHouse ReplacingMergeTree can deduplicate
    without ambiguity.

    Fields chosen as the natural key:
      - vendor_id: differentiates trips from different dispatch systems.
      - pickup_datetime + dropoff_datetime: define the trip window.
      - pickup_location_id: spatially anchors the trip start.

    The separator '|' is unlikely to appear in field values. ISO format
    for datetimes avoids locale-dependent string representations.
    """
    key = "|".join(
        [
            str(vendor_id),
            pickup_datetime.isoformat(),
            dropoff_datetime.isoformat(),
            str(pickup_location_id),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


class TripDeduplicator:
    """
    Detects duplicate trips within a single batch before they reach ClickHouse.

    Why this matters:
      If the same trip_id appears twice in one insert batch, both rows land
      in ClickHouse before ReplacingMergeTree has a chance to merge them.
      Until a background merge runs, queries without FINAL return a doubled
      count. Removing within-batch duplicates here eliminates that window.

    On Kafka replay:
      ReplacingMergeTree handles cross-batch deduplication (same trip_id
      from a previous insert arrives again). This class handles within-batch
      deduplication (same trip_id appears multiple times in one Parquet chunk).
    """

    def deduplicate(self, trips: list[Trip]) -> tuple[list[Trip], list[Trip]]:
        """
        Split trips into unique and duplicate lists.

        First occurrence of each trip_id is kept; subsequent occurrences
        are returned as duplicates. Order of unique trips is preserved.

        Returns:
            (unique_trips, duplicate_trips)
        """
        seen: set[str] = set()
        unique: list[Trip] = []
        duplicates: list[Trip] = []

        for trip in trips:
            if trip.trip_id in seen:
                duplicates.append(trip)
            else:
                seen.add(trip.trip_id)
                unique.append(trip)

        return unique, duplicates
