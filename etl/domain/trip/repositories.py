from __future__ import annotations

from abc import ABC, abstractmethod

from etl.domain.trip.models import Trip, Zone


class TripRepository(ABC):
    """
    Abstract interface for persisting Trip aggregates.

    Infrastructure provides the concrete implementation
    (ClickHouseTripRepository). The domain defines what it needs;
    infrastructure decides how. Swapping ClickHouse for another store
    means implementing this interface and rewiring the container.
    """

    @abstractmethod
    def save_batch(self, trips: list[Trip]) -> None:
        """
        Persist a batch of validated Trip aggregates.

        Implementations must be idempotent -- the same batch may arrive
        twice on Kafka replay. ClickHouse handles deduplication via
        ReplacingMergeTree on trip_id.
        """

    @abstractmethod
    def count(self) -> int:
        """Return the total number of trips stored (deduplicated)."""


class ZoneRepository(ABC):
    """
    Abstract interface for looking up Zone reference data.

    Infrastructure provides CsvZoneRepository, which loads the TLC
    taxi_zone_lookup.csv into memory once at startup. The enricher
    depends on this interface, not on the CSV implementation.
    """

    @abstractmethod
    def get_by_id(self, location_id: int) -> Zone:
        """
        Return the Zone for the given TLC location ID.

        Must never raise for an unknown location_id. Implementations
        return Zone.unknown(location_id) as the safe fallback so a
        missing zone never rejects an otherwise valid trip.
        """

    @abstractmethod
    def load_all(self) -> dict[int, Zone]:
        """Return all zones keyed by location_id."""
