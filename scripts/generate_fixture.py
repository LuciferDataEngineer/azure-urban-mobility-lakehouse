"""Create deterministic, wholly synthetic TLC-shaped taxi records; no real trips."""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.float64()),
    ("trip_distance", pa.float64()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("total_amount", pa.float64()),
], metadata={b"data_origin": b"wholly synthetic; not observed NYC taxi trips", b"fixture_contract": b"1"})


def trip(**changes):
    """One invented trip. Overrides create specific test scenarios."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 1, 10, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 10, 15),
        "passenger_count": 1.0,
        "trip_distance": 2.0,
        "PULocationID": 1,
        "DOLocationID": 2,
        "payment_type": 1,
        "fare_amount": 10.25,
        "total_amount": 12.30,
    }
    row.update(changes)
    return row


def sample_rows():
    """Six input rows: four accepted (one duplicate and one adjustment), two rejected."""
    return [
        trip(),
        trip(),
        trip(tpep_pickup_datetime=datetime(2024, 1, 1, 10, 30),
             tpep_dropoff_datetime=datetime(2024, 1, 1, 10, 45),
             trip_distance=1.0, fare_amount=-5.0, total_amount=-6.0),
        trip(tpep_pickup_datetime=datetime(2024, 1, 2, 12, 0),
             tpep_dropoff_datetime=datetime(2024, 1, 2, 12, 10),
             PULocationID=2, DOLocationID=3, trip_distance=0.0,
             fare_amount=7.0, total_amount=8.0),
        trip(tpep_dropoff_datetime=datetime(2024, 1, 1, 9, 59)),
        trip(tpep_pickup_datetime=None),
    ]


def write_fixture(path: Path, rows=None, *, schema=SCHEMA):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(sample_rows() if rows is None else rows, schema=schema)
    pq.write_table(table, path, compression="snappy", version="2.6", row_group_size=2,
                   store_schema=True, write_statistics=True)
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("fixtures/synthetic-yellow-2024-01.parquet"))
    args = parser.parse_args()
    print(write_fixture(args.output))


if __name__ == "__main__":
    main()
