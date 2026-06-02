from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

from etl.config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ZoneRecord:
    """
    Holds zone metadata for a single TLC location ID.

    Immutable by design -- zone data is static reference data loaded
    once at startup and never mutated during the pipeline run.
    """

    location_id: int
    borough: str
    zone: str
    service_zone: str

    @classmethod
    def unknown(cls, location_id: int) -> ZoneRecord:
        """
        Fallback record used when a location_id is not found.

        The enricher calls this instead of raising so a missing zone
        never crashes the pipeline. The trip is still valid -- it is
        enriched with 'Unknown' labels rather than rejected.
        """
        return cls(
            location_id=location_id,
            borough="Unknown",
            zone="Unknown",
            service_zone="Unknown",
        )


class CsvZoneRepository:
    """
    Implements the ZoneRepository interface using a CSV file as the source.

    The entire CSV (~265 rows) is loaded into a dict at construction time,
    giving O(1) lookup by location_id during enrichment. The memory
    footprint is negligible compared to the trip data being processed.

    The CSV format matches the TLC taxi_zone_lookup.csv:
        LocationID,Borough,Zone,service_zone

    Usage:
        repo = CsvZoneRepository()
        repo.load()
        zone = repo.get_by_id(132)   # JFK Airport
    """

    def __init__(self, path: Path | None = None) -> None:
        settings = get_settings()
        self._path = path or settings.etl.zone_lookup_path
        self._zones: dict[int, ZoneRecord] = {}
        self._loaded = False

    def load(self) -> None:
        """
        Read the CSV file and populate the in-memory lookup dict.

        Safe to call multiple times -- subsequent calls reload from disk,
        allowing hot-reload in long-running processes if needed.

        Raises:
            FileNotFoundError: if the CSV file does not exist.
            ValueError: if a row has an invalid LocationID.
        """
        if not self._path.exists():
            raise FileNotFoundError(f"Zone lookup CSV not found: {self._path}")

        zones: dict[int, ZoneRecord] = {}

        with open(self._path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    location_id = int(row["LocationID"])
                except (KeyError, ValueError) as exc:
                    logger.warning(
                        "Skipping invalid zone row",
                        extra={"row": dict(row), "error": str(exc)},
                    )
                    continue

                zones[location_id] = ZoneRecord(
                    location_id=location_id,
                    borough=row.get("Borough", "Unknown").strip(),
                    zone=row.get("Zone", "Unknown").strip(),
                    service_zone=row.get("service_zone", "Unknown").strip(),
                )

        self._zones = zones
        self._loaded = True

        logger.info(
            "Zone lookup loaded",
            extra={"path": str(self._path), "zone_count": len(self._zones)},
        )

    def get_by_id(self, location_id: int) -> ZoneRecord:
        """
        Return the ZoneRecord for the given location_id.

        Returns ZoneRecord.unknown() if the ID is not in the lookup
        rather than raising, so a stale or unexpected location ID
        never fails an otherwise valid trip.

        Raises:
            RuntimeError: if load() has not been called yet.
        """
        if not self._loaded:
            raise RuntimeError(
                "CsvZoneRepository.load() must be called before get_by_id(). "
                "Check the startup sequence in runtime/lifecycle.py."
            )

        record = self._zones.get(location_id)
        if record is None:
            logger.debug(
                "Zone ID not found, returning unknown",
                extra={"location_id": location_id},
            )
            return ZoneRecord.unknown(location_id)

        return record

    def load_all(self) -> dict[int, ZoneRecord]:
        """
        Return the full lookup dict.

        Primarily used for bulk operations and testing. Callers should
        treat the returned dict as read-only.

        Raises:
            RuntimeError: if load() has not been called yet.
        """
        if not self._loaded:
            raise RuntimeError(
                "CsvZoneRepository.load() must be called before load_all()."
            )
        return dict(self._zones)

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def zone_count(self) -> int:
        return len(self._zones)
