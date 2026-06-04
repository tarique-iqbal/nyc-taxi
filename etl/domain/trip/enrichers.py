from __future__ import annotations

from etl.domain.trip.models import Trip, Zone
from etl.domain.trip.repositories import ZoneRepository


class TripEnricher:
    """
    Resolves raw TLC location IDs to human-readable zone names and boroughs.

    Depends on ZoneRepository (abstract interface) -- the concrete CSV
    implementation is wired in the DI container. The enricher never
    touches the file system directly.

    Resolution strategy:
      - Lookup pickup_location_id and dropoff_location_id in the repository.
      - If a zone is found, populate the trip's zone and borough fields.
      - If not found, the repository returns Zone.unknown() and the trip
        receives 'Unknown' labels. The trip is NOT rejected.

    This matches the system design: a trip with an unresolvable zone is
    still a valid trip for analytics purposes.
    """

    def __init__(self, zone_repository: ZoneRepository) -> None:
        self._zone_repo = zone_repository

    def enrich(self, trip: Trip) -> None:
        """
        Populate zone and borough fields on the given Trip in place.

        Safe to call multiple times -- subsequent calls overwrite
        the previous zone values.
        """
        pickup_zone: Zone = self._zone_repo.get_by_id(trip.pickup_location_id)
        dropoff_zone: Zone = self._zone_repo.get_by_id(trip.dropoff_location_id)
        trip.enrich_zones(pickup_zone, dropoff_zone)

    def enrich_batch(self, trips: list[Trip]) -> None:
        """
        Enrich a list of trips. Each trip is enriched in place.

        Iterates once per trip. Zone lookups are O(1) dict hits since
        the repository holds an in-memory dict keyed by location_id.
        """
        for trip in trips:
            self.enrich(trip)
