# Databricks notebook source
"""Azure target implementation. Syntax checked locally; never run on Databricks yet.

Import this and contract.py into the same workspace directory. Requires a UC
external volume named raw and managed schema storage, DBR supporting TIMESTAMP_NTZ,
PyArrow, and one authorized writer. Run via the serialized job definition.
"""
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DecimalType, DoubleType, LongType, StringType, StructField,
    StructType, TimestampNTZType,
)
from contract import CONTRACT_VERSION, OPTIONAL, REQUIRED, normalize


def widget(name, default=''):
    dbutils.widgets.text(name, default)  # noqa: F821 - provided by Databricks
    return dbutils.widgets.get(name)  # noqa: F821


def main():
    catalog = widget('catalog', 'mobility')
    schema = widget('schema', 'yellow')
    month = widget('source_month', '2024-01')
    # /Volumes/<catalog>/<schema>/raw/yellow/2024-01/<ADF RunId>/source.parquet
    source_uri = widget('source_uri')
    expected_sha = widget('expected_sha256')
    expected_bytes = widget('expected_bytes')
    allow_revision = widget('allow_revision', 'false') == 'true'
    for value in [catalog, schema]:
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', value):
            raise ValueError('Catalog/schema must be lowercase SQL identifiers')
    if month not in {'2024-01', '2024-02', '2024-03'}:
        raise ValueError('Source month is outside the approved lab scope')
    prefix = f'/Volumes/{catalog}/{schema}/raw/yellow/{month}/'
    if not source_uri.startswith(prefix) or '..' in Path(source_uri).parts or not source_uri.endswith('/source.parquet'):
        raise ValueError('Only retained source files in this project raw volume are allowed')
    path = Path(source_uri)
    size = path.stat().st_size
    if not 8 <= size <= 256 * 1024 * 1024:
        raise ValueError('Raw Parquet must be between 8 bytes and 256 MiB')
    if expected_bytes and int(expected_bytes) != size:
        raise ValueError('Raw source byte count does not match the manifest')
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    sha = digest.hexdigest()
    if expected_sha and expected_sha != sha:
        raise ValueError('SHA256 mismatch: do not transform modified raw data')
    parquet = pq.ParquetFile(path)
    rows_expected = parquet.metadata.num_rows
    if rows_expected < 1 or rows_expected > 10_000_000:
        raise ValueError('Empty or oversized source row count')
    if len(parquet.schema_arrow) > 128 or parquet.metadata.num_row_groups > 5000:
        raise ValueError('Source exceeds column or row group limit')
    if len(set(n.casefold() for n in parquet.schema_arrow.names)) != len(parquet.schema_arrow.names):
        raise ValueError('Duplicate source column names are not supported')
    if {'file_row_number', 'filename', 'file_row_group_number'} & set(parquet.schema_arrow.names):
        raise ValueError('Reserved virtual column name')
    missing = REQUIRED - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError(f'Required schema fields are missing: {sorted(missing)}')
    for field_name in REQUIRED | OPTIONAL:
        if field_name not in parquet.schema_arrow.names:
            continue
        dtype = parquet.schema_arrow.field(field_name).type
        if field_name in {'tpep_pickup_datetime', 'tpep_dropoff_datetime'}:
            if not pa.types.is_timestamp(dtype) or dtype.tz is not None:
                raise ValueError(f'{field_name} must be a naive Arrow timestamp')
        elif not (pa.types.is_integer(dtype) or pa.types.is_floating(dtype) or pa.types.is_decimal(dtype)):
            raise ValueError(f'{field_name} must be an Arrow numeric type')
    additional = sorted(set(parquet.schema_arrow.names) - REQUIRED - OPTIONAL)
    schema_json = json.dumps({'arrow': str(parquet.schema_arrow), 'additional_columns': additional}, sort_keys=True)
    qualified = f'`{catalog}`.`{schema}`'
    bronze = f'{qualified}.bronze_versions'
    current = f'{qualified}.current_rows'
    stage = f'{qualified}._stage_{uuid4().hex}'
    spark.conf.set('spark.sql.session.timeZone', 'UTC')  # noqa: F821
    # NTZ stores naive civil values; UTC here never asserts NYC data are UTC.
    spark.conf.set('spark.databricks.delta.replaceWhere.constraintCheck.enabled', 'true')  # noqa: F821
    current_exists = spark.catalog.tableExists(current)  # noqa: F821
    if current_exists:
        committed = (spark.table(current).where(F.col('source_month') == month)  # noqa: F821
                     .select('source_sha256', 'contract_version').distinct().collect())
        if len(committed) == 1 and committed[0].source_sha256 == sha and committed[0].contract_version == CONTRACT_VERSION:
            result = {'status': 'replayed', 'source_month': month, 'sha256': sha, 'rows': rows_expected}
            print(json.dumps(result))
            dbutils.notebook.exit(json.dumps(result))  # noqa: F821
        if committed and not allow_revision:
            raise ValueError('Corrected month requires allow_revision=true after inspection')
    fields = StructType([
        StructField('source_month', StringType(), False),
        StructField('source_sha256', StringType(), False),
        StructField('source_row', LongType(), False),
        StructField('source_uri', StringType(), False),
        StructField('source_bytes', LongType(), False),
        StructField('captured_at_utc', StringType(), False),
        StructField('source_schema_json', StringType(), False),
        StructField('contract_version', StringType(), False),
        StructField('pickup_at', TimestampNTZType(), True),
        StructField('dropoff_at', TimestampNTZType(), True),
        StructField('pickup_zone_id', LongType(), True),
        StructField('dropoff_zone_id', LongType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('fare_amount', DecimalType(18, 2), True),
        StructField('total_amount', DecimalType(18, 2), True),
        StructField('vendor_id', DoubleType(), True),
        StructField('passenger_count', DoubleType(), True),
        StructField('payment_type', DoubleType(), True),
        StructField('negative_adjustment', BooleanType(), False),
        StructField('row_status', StringType(), False),
        StructField('reject_reasons', ArrayType(StringType(), False), False),
        StructField('raw_payload', StringType(), False),
    ])
    captured = datetime.now(timezone.utc).isoformat()
    version_filter = ((F.col('source_month') == month) & (F.col('source_sha256') == sha)
                      & (F.col('contract_version') == CONTRACT_VERSION))
    bronze_exists = spark.catalog.tableExists(bronze)  # noqa: F821
    version_exists = bronze_exists and spark.table(bronze).where(version_filter).limit(1).count() > 0  # noqa: F821
    try:
        if not version_exists:
            ordinal = 0
            for batch in parquet.iter_batches(batch_size=16384):
                normalized = []
                for row in batch.to_pylist():
                    ordinal += 1
                    normalized.append({
                        'source_month': month, 'source_sha256': sha, 'source_row': ordinal - 1,
                        'source_uri': source_uri, 'source_bytes': size, 'captured_at_utc': captured,
                        'source_schema_json': schema_json, 'contract_version': CONTRACT_VERSION,
                        **normalize(row, month),
                    })
                (spark.createDataFrame(normalized, fields).write.format('delta')  # noqa: F821
                 .mode('append').saveAsTable(stage))
            staged = spark.table(stage)  # noqa: F821
            if ordinal != rows_expected or staged.count() != rows_expected:
                raise ValueError('Source-to-stage row count failed')
            # One append commits the entire version, not individual batch commits.
            (staged.write.format('delta').mode('append')
             .partitionBy('source_month').saveAsTable(bronze))
        version = spark.table(bronze).where(version_filter)  # noqa: F821
        if version.count() != rows_expected:
            raise ValueError('Bronze version count mismatch; investigate before publication')
        # Every source row appears exactly once as regular, adjustment or quarantined.
        counts = {r.row_status: r['count'] for r in version.groupBy('row_status').count().collect()}
        if sum(counts.values()) != rows_expected or set(counts) - {'regular', 'adjustment', 'quarantined'}:
            raise ValueError('Conservation check failed')
        import math
        total_distance = version.where(F.col('row_status') != 'quarantined').agg(F.sum('trip_distance')).first()[0]
        if total_distance is not None and not math.isfinite(total_distance):
            raise ValueError('Accepted distance aggregation overflow; current slice unchanged')
        (version.write.format('delta').mode('overwrite').partitionBy('source_month')
         .option('replaceWhere', f"source_month = '{month}'").saveAsTable(current))
        # A crash before here leaves old Silver untouched; after here rerun is a no-op.
        # Views are installed separately during bootstrap, avoiding DDL in publication.
        result = {'status': 'committed', 'source_month': month, 'sha256': sha,
                  'source_bytes': size, 'rows': rows_expected, 'counts': counts,
                  'additional_columns': additional, 'contract_version': CONTRACT_VERSION}
        print(json.dumps(result, sort_keys=True))
        dbutils.notebook.exit(json.dumps(result))  # noqa: F821
    finally:
        spark.sql(f'DROP TABLE IF EXISTS {stage}')  # noqa: F821


if __name__ == '__main__':
    main()
