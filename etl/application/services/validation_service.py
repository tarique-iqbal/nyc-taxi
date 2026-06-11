from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)


class RawTripSchema(BaseModel):
    """
    Pydantic schema for raw rows coming off ParquetReader.

    This is application-level validation -- it runs before the domain
    pipeline and catches completely malformed records that would cause
    unrecoverable parse errors downstream. It is deliberately permissive
    on value ranges (domain validators enforce business rules later).

    Required fields: pickup_datetime, dropoff_datetime, pickup_location_id,
    dropoff_location_id. All other fields are optional with sensible defaults.
    """

    model_config = {"extra": "ignore"}

    pickup_datetime: datetime
    dropoff_datetime: datetime
    pickup_location_id: int
    dropoff_location_id: int

    vendor_id: Any | None = None
    passenger_count: float | None = None
    trip_distance: float | None = None
    rate_code_id: Any | None = None
    store_and_fwd_flag: str | None = None
    payment_type: Any | None = None

    fare_amount: float | None = None
    extra: float | None = None
    mta_tax: float | None = None
    tip_amount: float | None = None
    tolls_amount: float | None = None
    improvement_surcharge: float | None = None
    congestion_surcharge: float | None = None
    airport_fee: float | None = None
    total_amount: float | None = None

    @model_validator(mode="after")
    def dropoff_after_pickup(self) -> RawTripSchema:
        """Dropoff must not precede pickup."""
        if self.dropoff_datetime <= self.pickup_datetime:
            raise ValueError(
                f"dropoff_datetime ({self.dropoff_datetime}) must be after "
                f"pickup_datetime ({self.pickup_datetime})"
            )
        return self

    @field_validator("pickup_location_id", "dropoff_location_id", mode="before")
    @classmethod
    def location_id_positive(cls, v: Any) -> int:
        val = int(v)
        if val <= 0:
            raise ValueError(f"location_id must be positive, got {val}")
        return val


class ValidationService:
    """
    Application-level validation layer using Pydantic schema checking.

    Runs before the domain pipeline as the first line of defence against
    completely malformed records -- missing required timestamps, wrong types,
    null location IDs. Records that fail here are dead-lettered immediately
    without ever reaching the domain layer.

    Domain validators (TripValidator) handle business rules: duration limits,
    passenger count range, fare negativity. Both layers are intentionally
    separate so schema failures and business rule failures have different
    error types in the DLQ.
    """

    def validate_raw(
        self, row: dict[str, Any]
    ) -> tuple[bool, str | None]:
        """
        Validate a raw row against RawTripSchema.

        Returns:
            (True, None) if valid.
            (False, error_message) if invalid.
        """
        try:
            RawTripSchema.model_validate(row)
            return True, None
        except ValidationError as exc:
            error_summary = "; ".join(
                f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                for e in exc.errors()
            )
            return False, error_summary

    def validate_batch(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
        """
        Validate a list of raw rows.

        Returns:
            (valid_rows, [(invalid_row, error_message), ...])
        """
        valid: list[dict[str, Any]] = []
        invalid: list[tuple[dict[str, Any], str]] = []

        for row in rows:
            ok, error = self.validate_raw(row)
            if ok:
                valid.append(row)
            else:
                invalid.append((row, error or "Validation failed"))

        if invalid:
            logger.debug(
                "Schema validation rejected rows",
                extra={"total": len(rows), "rejected": len(invalid)},
            )

        return valid, invalid
