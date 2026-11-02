"""Pure Python cloud contract v1; driver-side reference, no Azure credentials.

A source row is not a trip ID. SHA256 + 0-based Parquet physical row ordinal
retains exact duplicates. This module deliberately has no Spark imports.
"""
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import math

CONTRACT_VERSION = '1'
REQUIRED = frozenset({
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID',
    'DOLocationID', 'trip_distance', 'fare_amount', 'total_amount',
})
OPTIONAL = frozenset({'VendorID', 'passenger_count', 'payment_type'})
CENT = Decimal('0.01')


def timestamp(value):
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    # NYC civil timestamps: do not silently attach a UTC offset or resolve DST.
    return result if result.tzinfo is None else None


def number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (ValueError, TypeError, OverflowError):
        return None


def optional_number(value):
    # Optional attributes are preserved, not used to reject a trip. Like local
    # TRY_CAST AS DOUBLE, NaN/Infinity remain values for duplicate grouping.
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError, OverflowError):
        return None


def money(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = Decimal(str(value))
        if not parsed.is_finite() or abs(parsed) >= Decimal('10000000000000000'):
            return None
        # Match the local DECIMAL(38,9) -> rounded DECIMAL(18,2) contract.
        rounded = parsed.quantize(Decimal('0.000000001'), rounding=ROUND_HALF_UP).quantize(CENT, rounding=ROUND_HALF_UP)
        return rounded if abs(rounded) < Decimal('10000000000000000') else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def location(value):
    parsed = number(value)
    # TLC 264/265 are unknown/outside-zone sentinel IDs, not invalid IDs.
    return int(parsed) if parsed is not None and parsed.is_integer() and 1 <= parsed <= 265 else None


def _json_value(value):
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    if isinstance(value, (date, datetime, time, Decimal)):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def normalize(row, source_month):
    """Return all rejection reasons and accepted business values, never drop a row."""
    pickup = timestamp(row.get('tpep_pickup_datetime'))
    dropoff = timestamp(row.get('tpep_dropoff_datetime'))
    pu, do = location(row.get('PULocationID')), location(row.get('DOLocationID'))
    distance = number(row.get('trip_distance'))
    fare, total = money(row.get('fare_amount')), money(row.get('total_amount'))
    reasons = []
    if pickup is None or dropoff is None:
        reasons.append('invalid_timestamp')
    if pickup is not None and pickup.strftime('%Y-%m') != source_month:
        reasons.append('pickup_outside_source_month')
    if pickup and dropoff and dropoff <= pickup:
        reasons.append('nonpositive_duration')
    if pu is None or do is None:
        reasons.append('invalid_location_id')
    if distance is None or distance < 0:
        reasons.append('invalid_trip_distance')
    if fare is None or total is None:
        reasons.append('invalid_money')
    negative_source_amount = any(number(row.get(k)) is not None and number(row.get(k)) < 0 for k in ['fare_amount', 'total_amount'])
    status = 'quarantined' if reasons else ('adjustment' if negative_source_amount else 'regular')
    return {
        'pickup_at': pickup, 'dropoff_at': dropoff,
        'pickup_zone_id': pu, 'dropoff_zone_id': do, 'trip_distance': distance,
        'fare_amount': fare, 'total_amount': total, 'row_status': status,
        'vendor_id': optional_number(row.get('VendorID')),
        'passenger_count': optional_number(row.get('passenger_count')),
        'payment_type': optional_number(row.get('payment_type')),
        'negative_adjustment': negative_source_amount,
        'reject_reasons': reasons,
        'raw_payload': json.dumps(_json_value(row), sort_keys=True, allow_nan=False, separators=(',', ':')),
    }
