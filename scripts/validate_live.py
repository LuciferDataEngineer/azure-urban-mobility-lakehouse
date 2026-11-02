"""Check retained source lineage, business-state replay and aggregate export integrity.

No downloads. Run after the three bounded fetch/ingest commands in README.
Fingerprints are checks, not cryptographic proof of row-set equality.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import platform

import duckdb
import pyarrow
import pyarrow.parquet as pq
from mobility_lakehouse.pipeline import ingest, report


TABLES = ('sources', 'current_sources', 'silver_trips', 'quarantine', 'gold_hourly')


def fingerprints(data_dir):
    with duckdb.connect(str(data_dir / 'warehouse.duckdb'), read_only=True) as con:
        return {table: list(con.execute(
            f'SELECT count(*),bit_xor(hash(t)),sum(hash(t)::DECIMAL(38,0))::VARCHAR FROM {table} t'
        ).fetchone()) for table in TABLES}


def validate(data_dir):
    with duckdb.connect(str(data_dir / 'warehouse.duckdb'), read_only=True) as con:
        sources = con.execute('''SELECT source_month,source_sha256,raw_relative_path,
            source_url,raw_rows,accepted_rows,quarantined_rows FROM current_source_versions
            ORDER BY source_month''').fetchall()
        if [r[0] for r in sources] != ['2024-01', '2024-02', '2024-03']:
            raise ValueError('validation requires exactly the three real historical source months')
        evidence = []
        for month,sha,path,url,count,accepted,quarantined in sources:
            raw = data_dir / path
            h = hashlib.sha256()
            with raw.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    h.update(block)
            assert h.hexdigest() == sha
            with pq.ParquetFile(raw) as parquet:
                assert parquet.metadata.num_rows == count == accepted + quarantined
            # Physical rows are exactly partitioned into current Silver and quarantine.
            physical = con.execute('''WITH all_rows AS (
                SELECT source_row FROM silver_trips WHERE source_month=?
                UNION ALL SELECT source_row FROM quarantine WHERE source_month=?)
                SELECT count(*),count(DISTINCT source_row),min(source_row),max(source_row) FROM all_rows
            ''', [month,month]).fetchone()
            assert physical == (count,count,0,count-1)
            assert con.execute('''SELECT count(*) FROM silver_trips
                WHERE source_month=? AND source_sha256<>?''',[month,sha]).fetchone()[0] == 0
            assert con.execute('''SELECT count(*) FROM quarantine
                WHERE source_month=? AND source_sha256<>?''',[month,sha]).fetchone()[0] == 0
            totals = con.execute('''SELECT count(*),sum(fare_amount),sum(total_amount)
                FROM silver_trips WHERE source_month=?''',[month]).fetchone()
            gold = con.execute('''SELECT sum(trip_count),sum(fare_amount_sum),sum(total_amount_sum)
                FROM gold_hourly WHERE source_month=?''',[month]).fetchone()
            assert totals == gold
            evidence.append({'month':month,'source_sha256':sha,'source_url':url,
                             'raw_rows':count,'accepted_rows':accepted,'quarantined_rows':quarantined,
                             'signed_fare_usd':str(totals[1]),'signed_total_usd':str(totals[2])})
    before = fingerprints(data_dir)
    replays = [ingest(data_dir / row[2], row[0], data_dir, source_url=row[3]) for row in sources]
    assert all(r['replayed'] for r in replays)
    after = fingerprints(data_dir)
    assert before == after
    output = report(data_dir)
    generation = data_dir / 'reports' / 'generations' / output['generation']
    export_counts = {}
    with duckdb.connect(str(data_dir / 'warehouse.duckdb'), read_only=True) as con:
        for name,sql in [('hourly','SELECT * FROM gold_hourly ORDER BY source_month,pickup_hour,pickup_zone_id,negative_adjustment'),
                         ('quality','SELECT * FROM quality_summary ORDER BY source_month,reason')]:
            cursor = con.execute(sql)
            expected_header = [c[0] for c in cursor.description]
            with (generation / f'{name}.csv').open(newline='') as stream:
                reader = csv.reader(stream)
                assert next(reader) == expected_header
                count = 0
                for batch in iter(lambda: cursor.fetchmany(10000), []):
                    for row in batch:
                        assert next(reader) == [str(value) if value is not None else '' for value in row]
                        count += 1
                assert next(reader, None) is None
            export_counts[name] = count
    return {'execution':'local only; no Azure resources deployed',
            'python':platform.python_version(),'duckdb':duckdb.__version__,'pyarrow':pyarrow.__version__,
            'source_checks':evidence,'all_source_rows_partitioned_exactly_once':True,
            'signed_decimal_silver_gold_reconciled':True,'identical_source_replays':len(replays),
            'business_table_fingerprints_unchanged':True,'fingerprints':after,
            'csv_rows_exactly_compared':export_counts,'totals':output['totals']}


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir',type=Path,default=Path('data/live'))
    args=parser.parse_args()
    print(json.dumps(validate(args.data_dir),indent=2,allow_nan=False))
