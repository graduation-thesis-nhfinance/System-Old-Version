"""
Data Quality Validation Gates for ETL Tasks
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

REQUIRED_KLINE_FIELDS = ("open", "high", "low", "close", "volume")
TIME_FIELDS = ("open_time", "timestamp")


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_timestamp(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
    return None


def _record_time(record: Dict[str, Any]) -> Optional[float]:
    for field in TIME_FIELDS:
        ts = _as_timestamp(record.get(field))
        if ts is not None:
            return ts
    return None


def validate_klines_data(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validates Binance Kline records before they can move into Silver/Gold."""
    if not records:
        return {
            "passed": False,
            "total_records": 0,
            "failed_records": 0,
            "errors": ["Record batch is empty"],
            "status": "FAILED"
        }

    missing_required_count = 0
    invalid_numeric_count = 0
    null_close_count = 0
    negative_volume_count = 0
    invalid_ohlc_count = 0
    missing_timestamp_count = 0
    duplicate_key_count = 0
    interval_gap_count = 0
    errors = []
    seen_keys = set()
    timestamps = []

    for record in records:
        missing_fields = [field for field in REQUIRED_KLINE_FIELDS if field not in record]
        if missing_fields:
            missing_required_count += 1
            errors.append(f"Missing required fields: {','.join(missing_fields)}")

        open_price = _as_float(record.get("open"))
        high = _as_float(record.get("high"))
        low = _as_float(record.get("low"))
        close = _as_float(record.get("close"))
        volume = _as_float(record.get("volume"))

        numeric_values = [open_price, high, low, close, volume]
        if any(value is None for value in numeric_values):
            invalid_numeric_count += 1

        if close is None:
            null_close_count += 1

        if volume is not None and volume < 0:
            negative_volume_count += 1

        if all(value is not None for value in [open_price, high, low, close]):
            if high < low or high < open_price or high < close or low > open_price or low > close:
                invalid_ohlc_count += 1

        timestamp = _record_time(record)
        if timestamp is None:
            missing_timestamp_count += 1
        else:
            timestamps.append(timestamp)

        symbol = record.get("symbol", "")
        key = (symbol, timestamp)
        if timestamp is not None:
            if key in seen_keys:
                duplicate_key_count += 1
            seen_keys.add(key)

    sorted_timestamps = sorted(set(timestamps))
    if len(sorted_timestamps) >= 2:
        deltas = [
            sorted_timestamps[idx] - sorted_timestamps[idx - 1]
            for idx in range(1, len(sorted_timestamps))
        ]
        positive_deltas = [delta for delta in deltas if delta > 0]
        if positive_deltas:
            expected_interval = min(positive_deltas)
            interval_gap_count = sum(1 for delta in positive_deltas if delta > expected_interval * 1.5)

    failed_records = sum([
        missing_required_count,
        invalid_numeric_count,
        null_close_count,
        negative_volume_count,
        invalid_ohlc_count,
        missing_timestamp_count,
        duplicate_key_count,
        interval_gap_count,
    ])

    passed = failed_records == 0

    result = {
        "passed": passed,
        "total_records": len(records),
        "failed_records": failed_records,
        "missing_required_count": missing_required_count,
        "invalid_numeric_count": invalid_numeric_count,
        "null_close_count": null_close_count,
        "negative_volume_count": negative_volume_count,
        "invalid_ohlc_count": invalid_ohlc_count,
        "missing_timestamp_count": missing_timestamp_count,
        "duplicate_key_count": duplicate_key_count,
        "interval_gap_count": interval_gap_count,
        "errors": sorted(set(errors)),
        "status": "PASSED" if passed else "FAILED"
    }
    logger.info(f"Data Quality Gate Check Result: {result}")
    return result
