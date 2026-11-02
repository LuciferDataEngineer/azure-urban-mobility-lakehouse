"""Run the repository's read-only SQL and a cardinality-safe reference-zone join."""
import argparse
import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa


def serialize(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def query(conn, sql):
    result = conn.execute(sql)
    names = [item[0] for item in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def run(data_dir):
    root = Path(__file__).resolve().parents[1]
    reference = root / 'reference' / 'taxi_zone_lookup.csv'
    manifest = json.loads((root / 'reference' / 'manifest.json').read_text())
    if hashlib.sha256(reference.read_bytes()).hexdigest() != manifest['sha256']:
        raise ValueError('zone snapshot checksum mismatch')
    with reference.open(newline='') as stream:
        zones = list(csv.DictReader(stream))
    ids = [int(row['LocationID']) for row in zones]
    if len(ids) != 265 or set(ids) != set(range(1, 266)):
        raise ValueError('zone lookup must have exactly one row per ID 1..265')
    for row in zones:
        row['LocationID'] = int(row['LocationID'])
    with duckdb.connect(str(Path(data_dir) / 'warehouse.duckdb'), read_only=True) as conn:
        conn.execute('BEGIN TRANSACTION')
        statements = conn.extract_statements((root / 'sql' / 'analysis.sql').read_text())
        if any(s.type != duckdb.StatementType.SELECT for s in statements):
            raise ValueError('analysis.sql must contain SELECT statements only')
        results = {f'query_{index}': query(conn, statement.query)
                   for index, statement in enumerate(statements, 1)}
        conn.register('taxi_zones', pa.Table.from_pylist(zones))
        results['top_pickup_zones'] = query(conn, '''
            SELECT g.pickup_zone_id,z.Borough,z.Zone,SUM(g.trip_count) AS records,
                   SUM(g.total_amount_sum) AS recorded_charges_usd
            FROM gold_hourly g LEFT JOIN taxi_zones z ON g.pickup_zone_id=z.LocationID
            WHERE NOT g.negative_adjustment
            GROUP BY g.pickup_zone_id,z.Borough,z.Zone
            ORDER BY records DESC,g.pickup_zone_id LIMIT 10
        ''')
        before = conn.execute('SELECT SUM(trip_count) FROM gold_hourly').fetchone()[0]
        after, missing = conn.execute('''
            SELECT SUM(g.trip_count),COUNT(*) FILTER (WHERE z.LocationID IS NULL)
            FROM gold_hourly g LEFT JOIN taxi_zones z ON g.pickup_zone_id=z.LocationID
        ''').fetchone()
        if before != after or missing:
            raise ValueError('zone join changed record counts or left unmatched zones')
        results['zone_join_validation'] = {'records_before': before, 'records_after': after,
                                           'unmatched_hourly_groups': missing,
                                           'lookup_sha256': manifest['sha256']}
        conn.execute('COMMIT')
    return json.loads(json.dumps(results, default=serialize, allow_nan=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path('data'))
    args = parser.parse_args()
    print(json.dumps(run(args.data_dir), indent=2, allow_nan=False))
