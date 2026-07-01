from __future__ import annotations

# Lookup tables
#
# Lookup values derived from the NYC Taxi & Limousine Commission
# Trip Record Data Dictionary.

VENDOR_MAP: dict[int, str] = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc.",
}

PAYMENT_TYPE_MAP: dict[int, str] = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATE_CODE_MAP: dict[int, str] = {
    1: "Standard",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
}

DEFAULT_PASSENGER_COUNT: int = 1
UNKNOWN_LABEL: str = "Unknown"


class TripNormalizer:
    """
    Maps raw TLC numeric codes to human-readable strings and applies
    default values for nullable fields.

    Operates on the raw dict produced by ParquetReader before the Trip
    entity is constructed. Returns a new dict -- does not mutate the input.

    This is pure Python with no I/O or external dependencies so it
    can be tested without any infrastructure.
    """

    @staticmethod
    def normalize(raw: dict[str, object]) -> dict[str, object]:
        """
        Return a new dict with all normalised values applied.

        Steps:
          1. vendor_id: int code -> readable string
          2. payment_type: int code -> readable string
          3. rate_code: int/float code -> readable string
          4. passenger_count: null -> 1
          5. store_and_fwd_flag: Y/N -> "Yes"/"No", null -> "Unknown"
        """
        normalised = dict(raw)

        normalised["vendor_id"] = TripNormalizer._normalise_vendor(raw.get("vendor_id"))
        normalised["payment_type"] = TripNormalizer._normalise_payment_type(raw.get("payment_type"))
        normalised["rate_code"] = TripNormalizer._normalise_rate_code(raw.get("rate_code_id"))
        normalised["passenger_count"] = TripNormalizer._normalise_passenger_count(
            raw.get("passenger_count")
        )
        normalised["store_and_fwd_flag"] = TripNormalizer._normalise_store_and_fwd(
            raw.get("store_and_fwd_flag")
        )

        return normalised

    @staticmethod
    def _to_int(value: object) -> int | None:
        """Safely convert a supported value to int."""
        if isinstance(value, (int, float, str)):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
        return None

    @staticmethod
    def _normalise_vendor(value: object) -> str:
        code = TripNormalizer._to_int(value)
        if code is None:
            return UNKNOWN_LABEL
        return VENDOR_MAP.get(code, UNKNOWN_LABEL)

    @staticmethod
    def _normalise_payment_type(value: object) -> str:
        code = TripNormalizer._to_int(value)
        if code is None:
            return UNKNOWN_LABEL
        return PAYMENT_TYPE_MAP.get(code, UNKNOWN_LABEL)

    @staticmethod
    def _normalise_rate_code(value: object) -> str:
        try:
            code = int(float(str(value)))
            return RATE_CODE_MAP.get(code, UNKNOWN_LABEL)
        except (TypeError, ValueError):
            return UNKNOWN_LABEL

    @staticmethod
    def _normalise_passenger_count(value: object) -> int:
        if value is None:
            return DEFAULT_PASSENGER_COUNT

        count = TripNormalizer._to_int(value)
        if count is None:
            return DEFAULT_PASSENGER_COUNT

        return count if count > 0 else DEFAULT_PASSENGER_COUNT

    @staticmethod
    def _normalise_store_and_fwd(value: object) -> str:
        if value is None:
            return UNKNOWN_LABEL

        mapping = {
            "Y": "Yes",
            "N": "No",
            "y": "Yes",
            "n": "No",
        }

        return mapping.get(str(value), UNKNOWN_LABEL)
