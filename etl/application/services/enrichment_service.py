from __future__ import annotations

import logging

from etl.domain.trip.enrichers import TripEnricher
from etl.domain.trip.models import Trip
from etl.domain.trip.repositories import ZoneRepository

logger = logging.getLogger(__name__)


class EnrichmentService:
    """
    Application-level enrichment coordinator.

    Wraps TripEnricher (domain) and ensures ZoneRepository is loaded
    before any enrichment is attempted. The domain enricher depends on
    ZoneRepository as an abstract interface; this service holds the
    concrete repository reference and guards its lifecycle.

    Responsibilities:
      - Verify the repository is loaded before delegating to the enricher.
      - Provide a reload() method for long-running processes that need to
        pick up a refreshed zone CSV without restarting (e.g. new zones
        added after a TLC zone boundary update).
      - Could be extended to enrich from external APIs (weather, events)
        in future -- the domain enricher stays pure, extra enrichment
        is added here.

    Usage:
        service = EnrichmentService(zone_repository=repo)
        service.ensure_loaded()
        service.enrich_batch(valid_trips)
    """

    def __init__(self, zone_repository: ZoneRepository) -> None:
        self._zone_repo = zone_repository
        self._enricher = TripEnricher(zone_repository)

    def ensure_loaded(self) -> None:
        """
        Assert the zone repository is loaded.

        Raises RuntimeError if the repository has not been loaded.
        Called at startup via lifecycle.py before the pipeline starts.
        """
        if not getattr(self._zone_repo, "is_loaded", True):
            raise RuntimeError(
                "ZoneRepository is not loaded. "
                "Call zone_repository.load() during startup sequence."
            )

    def enrich_batch(self, trips: list[Trip]) -> None:
        """
        Enrich a list of Trip entities in place with zone metadata.

        Each Trip's pickup_zone, dropoff_zone, pickup_borough, and
        dropoff_borough fields are populated from the zone repository.
        Trips with unknown location IDs receive 'Unknown' labels and
        are not rejected.
        """
        self._enricher.enrich_batch(trips)
        logger.debug(
            "Enriched trip batch",
            extra={"count": len(trips)},
        )

    def reload(self) -> None:
        """
        Reload zone data from the CSV source.

        Triggers a fresh load of the zone CSV into the repository.
        Only meaningful for CsvZoneRepository -- other implementations
        may be no-ops.
        """
        if hasattr(self._zone_repo, "load"):
            self._zone_repo.load()  # type: ignore[union-attr]
            logger.info(
                "Zone repository reloaded",
                extra={"zone_count": getattr(self._zone_repo, "zone_count", "unknown")},
            )

    @property
    def zone_count(self) -> int:
        return getattr(self._zone_repo, "zone_count", 0)
