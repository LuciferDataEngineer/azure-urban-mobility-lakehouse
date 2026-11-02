"""Immutable source files, transactional monthly slices, and reproducible serving outputs.

One writer (including report publication) per data directory is required. DuckDB's
file lock rejects conflicting processes; callers must serialize thread-level calls.
No automatic correction is made to source timestamps, prices or duplicate records.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import tempfile
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import uuid

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

LOG = logging.getLogger(__name__)
MAX_SOURCE_BYTES = 150 * 1024 * 1024
MAX_SOURCE_ROWS = 20_000_000
ALLOWED_FETCH_MONTHS = ("2024-01", "2024-02", "2024-03")
SOURCE_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
REQUIRED = (
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_distance", "fare_amount", "total_amount",
)
OPTIONAL = ("VendorID", "passenger_count", "payment_type")
CONTRACT_VERSION = "tlc-yellow-local-v1"


class ContractError(ValueError):
    """The retained source cannot be processed under the versioned contract."""


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def _json_text(value) -> str:
    return json.dumps(value, indent=2, default=_json_default, allow_nan=False) + "\n"


def _month(value: str) -> tuple[date, date]:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise ContractError("month must be YYYY-MM")
    year, month = map(int, value.split("-"))
    if not 2000 <= year <= 2100:
        raise ContractError("month year must be between 2000 and 2100")
    start = date(year, month, 1)
    end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return start, end


def _source_url(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        raise ContractError("source_url must be a public HTTPS URL without credentials, query or fragment")
    return value


def _connect(data_dir: Path, *, read_only: bool = False):
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(data_dir / "warehouse.duckdb"), read_only=read_only)
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '1GB'")
    if not read_only:
        con.execute("SET temp_directory = ?", [str(data_dir / "duckdb-tmp")])
        _initialize(con)
    return con


def _initialize(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS contract_metadata (contract_version VARCHAR PRIMARY KEY);
    INSERT INTO contract_metadata SELECT 'tlc-yellow-local-v1'
      WHERE NOT EXISTS (SELECT 1 FROM contract_metadata);
    CREATE TABLE IF NOT EXISTS run_audit (
      run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL,
      finished_at TIMESTAMP, source_month VARCHAR NOT NULL,
      source_sha256 VARCHAR, status VARCHAR NOT NULL, replayed BOOLEAN DEFAULT false,
      error_type VARCHAR
    );
    CREATE TABLE IF NOT EXISTS sources (
      source_month VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
      source_url VARCHAR, captured_at TIMESTAMP NOT NULL, file_bytes BIGINT NOT NULL,
      raw_relative_path VARCHAR NOT NULL, schema_json VARCHAR NOT NULL,
      contract_version VARCHAR NOT NULL, raw_rows BIGINT NOT NULL,
      accepted_rows BIGINT NOT NULL, quarantined_rows BIGINT NOT NULL,
      candidate_duplicate_rows BIGINT NOT NULL, negative_adjustment_rows BIGINT NOT NULL,
      committed_at TIMESTAMP NOT NULL, run_id VARCHAR NOT NULL,
      PRIMARY KEY (source_month, source_sha256),
      CHECK(raw_rows = accepted_rows + quarantined_rows)
    );
    CREATE TABLE IF NOT EXISTS current_sources (
      source_month VARCHAR PRIMARY KEY, source_sha256 VARCHAR NOT NULL
    );
    CREATE TABLE IF NOT EXISTS silver_trips (
      source_month VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
      source_row BIGINT NOT NULL, pickup_at TIMESTAMP NOT NULL, dropoff_at TIMESTAMP NOT NULL,
      pickup_zone_id INTEGER NOT NULL, dropoff_zone_id INTEGER NOT NULL,
      trip_distance DOUBLE NOT NULL, fare_amount DECIMAL(18,2) NOT NULL,
      total_amount DECIMAL(18,2) NOT NULL, vendor_id DOUBLE,
      passenger_count DOUBLE, payment_type DOUBLE,
      negative_adjustment BOOLEAN NOT NULL, candidate_duplicate BOOLEAN NOT NULL,
      PRIMARY KEY(source_month, source_sha256, source_row)
    );
    CREATE INDEX IF NOT EXISTS silver_pickup ON silver_trips(pickup_zone_id, pickup_at);
    CREATE TABLE IF NOT EXISTS quarantine (
      source_month VARCHAR NOT NULL, source_sha256 VARCHAR NOT NULL,
      source_row BIGINT NOT NULL, reasons VARCHAR[] NOT NULL,
      PRIMARY KEY(source_month, source_sha256, source_row)
    );
    CREATE TABLE IF NOT EXISTS gold_hourly (
      source_month VARCHAR NOT NULL, pickup_hour TIMESTAMP NOT NULL,
      pickup_zone_id INTEGER NOT NULL, negative_adjustment BOOLEAN NOT NULL,
      trip_count BIGINT NOT NULL, trip_distance_sum DOUBLE NOT NULL,
      duration_seconds_sum BIGINT NOT NULL, fare_amount_sum DECIMAL(38,2) NOT NULL,
      total_amount_sum DECIMAL(38,2) NOT NULL, candidate_duplicate_rows BIGINT NOT NULL,
      PRIMARY KEY(source_month, pickup_hour, pickup_zone_id, negative_adjustment)
    );
    CREATE VIEW IF NOT EXISTS current_source_versions AS
      SELECT s.* FROM sources s JOIN current_sources c USING(source_month,source_sha256);
    CREATE VIEW IF NOT EXISTS quality_summary AS
      SELECT source_month, reason, COUNT(*) AS row_count
      FROM quarantine, UNNEST(reasons) AS r(reason) GROUP BY source_month, reason;
    """)
    versions = con.execute("SELECT contract_version FROM contract_metadata").fetchall()
    if versions != [(CONTRACT_VERSION,)]:
        raise ContractError("warehouse contract differs; use a separate data directory or reviewed migration")


def _digest(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def _archive(file_path: Path, month: str, data_dir: Path, source_url: str | None):
    # Copy and hash the *same* byte stream. A caller replacing the input pathname
    # cannot associate one version's digest with another version's copied bytes.
    raw = data_dir / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".capture-", suffix=".parquet", dir=raw)
    temp = Path(temp_name)
    sha = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, "wb") as target, file_path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                size += len(chunk)
                if size > MAX_SOURCE_BYTES:
                    raise ContractError(f"source exceeds {MAX_SOURCE_BYTES} byte limit")
                sha.update(chunk)
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        if size == 0:
            raise ContractError("source is empty")
        digest = sha.hexdigest()
        directory = raw / month / digest
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / "source.parquet"
        if destination.exists():
            if _digest(destination) != digest:
                raise ContractError("retained raw object checksum mismatch; restore it before replay")
            temp.unlink()
        else:
            os.replace(temp, destination)
        manifest = directory / "manifest.json"
        if manifest.exists():
            metadata = json.loads(manifest.read_text())
            if (metadata.get("source_sha256") != digest
                    or metadata.get("file_bytes") != size or metadata.get("source_month") != month):
                raise ContractError("retained manifest identity mismatch")
        else:
            metadata = {
                "source_month": month, "source_sha256": digest, "file_bytes": size,
                "source_url": source_url, "captured_at": _utc(),
                "raw_relative_path": destination.relative_to(data_dir).as_posix(),
            }
            temp_manifest = directory / ".manifest.json.tmp"
            temp_manifest.write_text(_json_text(metadata))
            os.replace(temp_manifest, manifest)
        return destination, metadata
    finally:
        temp.unlink(missing_ok=True)


def _validate_schema(path: Path):
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        count = parquet.metadata.num_rows
        if count > MAX_SOURCE_ROWS or len(schema) > 128 or parquet.metadata.num_row_groups > 5000:
            raise ContractError("Parquet exceeds bounded row, column or row-group limits")
        if count == 0:
            raise ContractError("empty monthly files require an explicit deletion workflow")
        names = schema.names
        if len(names) != len(set(names)) or len(names) != len(set(n.casefold() for n in names)):
            raise ContractError("duplicate or case-colliding Parquet columns")
        if any(name in names for name in ("file_row_number", "filename", "file_row_group_number")):
            raise ContractError("reserved Parquet virtual column name")
        missing = sorted(set(REQUIRED) - set(names))
        if missing:
            raise ContractError("missing required columns: " + ", ".join(missing))
        for name in REQUIRED[:2]:
            typ = schema.field(name).type
            if not pa.types.is_timestamp(typ) or typ.tz is not None:
                raise ContractError(f"{name} must be a timezone-naive Arrow timestamp")
        for name in REQUIRED[2:] + OPTIONAL:
            if name not in names:
                continue
            typ = schema.field(name).type
            if not (pa.types.is_integer(typ) or pa.types.is_floating(typ) or pa.types.is_decimal(typ)):
                raise ContractError(f"{name} must have numeric Arrow type")
        document = {
            "contract_version": CONTRACT_VERSION,
            "fields": [{"name": field.name, "type": str(field.type), "nullable": field.nullable}
                       for field in schema],
            "extra_columns": sorted(set(names) - set(REQUIRED) - set(OPTIONAL)),
        }
        return names, count, json.dumps(document, sort_keys=True)
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError("cannot read valid Parquet metadata") from exc


def _stage(con, path: Path, names: list[str], start: date, end: date):
    optional_sql = ", ".join(
        f'TRY_CAST("{name}" AS DOUBLE) AS {alias}' if name in names else f"NULL::DOUBLE AS {alias}"
        for name, alias in zip(OPTIONAL, ("vendor_id", "passenger_count", "payment_type"))
    )
    con.execute(f"""
      CREATE TEMP TABLE staged AS
      WITH input AS (
        SELECT file_row_number AS source_row,
          TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_at,
          TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
          TRY_CAST(PULocationID AS DOUBLE) AS raw_pickup_zone,
          TRY_CAST(DOLocationID AS DOUBLE) AS raw_dropoff_zone,
          TRY_CAST(PULocationID AS INTEGER) AS pickup_zone_id,
          TRY_CAST(DOLocationID AS INTEGER) AS dropoff_zone_id,
          TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
          TRY_CAST(fare_amount AS DOUBLE) AS raw_fare,
          TRY_CAST(total_amount AS DOUBLE) AS raw_total,
          TRY_CAST(ROUND(TRY_CAST(fare_amount AS DECIMAL(38,9)),2) AS DECIMAL(18,2)) AS fare_amount,
          TRY_CAST(ROUND(TRY_CAST(total_amount AS DECIMAL(38,9)),2) AS DECIMAL(18,2)) AS total_amount,
          {optional_sql}
        FROM read_parquet(?, file_row_number=true, hive_partitioning=false)
      )
      SELECT *,
        COALESCE(raw_fare < 0 OR raw_total < 0, false) AS negative_adjustment,
        list_filter([
          CASE WHEN pickup_at IS NULL OR dropoff_at IS NULL
             OR NOT isfinite(pickup_at) OR NOT isfinite(dropoff_at) THEN 'invalid_timestamp' END,
          CASE WHEN pickup_at < ? OR pickup_at >= ? THEN 'pickup_outside_source_month' END,
          CASE WHEN dropoff_at <= pickup_at THEN 'nonpositive_duration' END,
          CASE WHEN raw_pickup_zone IS NULL OR NOT isfinite(raw_pickup_zone)
             OR raw_pickup_zone != trunc(raw_pickup_zone) OR raw_pickup_zone NOT BETWEEN 1 AND 265
             OR raw_dropoff_zone IS NULL OR NOT isfinite(raw_dropoff_zone)
             OR raw_dropoff_zone != trunc(raw_dropoff_zone) OR raw_dropoff_zone NOT BETWEEN 1 AND 265
             THEN 'invalid_location_id' END,
          CASE WHEN trip_distance IS NULL OR NOT isfinite(trip_distance) OR trip_distance < 0
             THEN 'invalid_trip_distance' END,
          CASE WHEN raw_fare IS NULL OR NOT isfinite(raw_fare) OR fare_amount IS NULL
             OR raw_total IS NULL OR NOT isfinite(raw_total) OR total_amount IS NULL
             THEN 'invalid_money' END
        ], lambda reason: reason IS NOT NULL) AS reasons
      FROM input
    """, [str(path), start, end])
    con.execute("""
      CREATE TEMP TABLE accepted AS
      SELECT source_row, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
        trip_distance, fare_amount, total_amount, vendor_id, passenger_count, payment_type,
        negative_adjustment,
        ROW_NUMBER() OVER (PARTITION BY pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
          trip_distance, fare_amount, total_amount, vendor_id, passenger_count, payment_type,
          negative_adjustment ORDER BY source_row) > 1 AS candidate_duplicate
      FROM staged WHERE len(reasons) = 0
    """)


def _hook(callback: Callable[[str], None] | None, phase: str):
    if callback:
        callback(phase)


def ingest(file_path, month: str, data_dir, source_url: str | None = None, *,
           allow_revert: bool = False, failure_hook: Callable[[str], None] | None = None) -> dict:
    """Retain one source and atomically replace its monthly current-state slice.

    Old versions remain in sources/raw; an explicitly approved revert can reactivate
    one. It is never inferred from arrival order. Failed transforms preserve the old
    month. A post-commit export failure is repaired with report() or identical replay.
    """
    start, end = _month(month)
    source_url = _source_url(source_url)
    data_dir = Path(data_dir)
    file_path = Path(file_path)
    run_id = str(uuid.uuid4())
    con = _connect(data_dir)
    con.execute("INSERT INTO run_audit(run_id,started_at,source_month,status) VALUES (?, ?, ?, 'running')",
                [run_id, _utc(), month])
    committed = False
    in_transaction = False
    result = None
    try:
        raw_path, metadata = _archive(file_path, month, data_dir, source_url)
        sha = metadata["source_sha256"]
        con.execute("UPDATE run_audit SET source_sha256=? WHERE run_id=?", [sha, run_id])
        _hook(failure_hook, "after_raw")
        current = con.execute("SELECT source_sha256 FROM current_sources WHERE source_month=?", [month]).fetchone()
        previous = con.execute("SELECT raw_rows,accepted_rows,quarantined_rows,candidate_duplicate_rows,negative_adjustment_rows FROM sources WHERE source_month=? AND source_sha256=?", [month, sha]).fetchone()
        if current == (sha,):
            if previous is None:
                raise ContractError("current source reference missing version metadata")
            result = dict(zip(("raw_rows", "accepted_rows", "quarantined_rows", "candidate_duplicate_rows", "negative_adjustment_rows"), previous))
            result.update(month=month, source_sha256=sha, replayed=True)
            committed = True
        else:
            if previous and not allow_revert:
                raise ContractError("historical version requires explicit allow_revert=True")
            names, raw_rows, schema_json = _validate_schema(raw_path)
            _stage(con, raw_path, names, start, end)
            _hook(failure_hook, "after_stage")
            accepted, duplicate_rows, negative_rows = con.execute("SELECT COUNT(*), count(*) FILTER(WHERE candidate_duplicate), count(*) FILTER(WHERE negative_adjustment) FROM accepted").fetchone()
            quarantined = con.execute("SELECT COUNT(*) FROM staged WHERE len(reasons)>0").fetchone()[0]
            if accepted + quarantined != raw_rows:
                raise ContractError("accepted plus quarantined row conservation failed")
            # Finite individual doubles can overflow when aggregated. Because all
            # accepted distances are nonnegative, a finite monthly total also
            # bounds every hourly subtotal. Do this before replacing any state.
            total_distance = con.execute("SELECT SUM(trip_distance) FROM accepted").fetchone()[0]
            if total_distance is not None and not math.isfinite(total_distance):
                raise ContractError("monthly aggregate trip distance is not finite")
            result = {"month": month, "source_sha256": sha, "raw_rows": raw_rows,
                      "accepted_rows": accepted, "quarantined_rows": quarantined,
                      "candidate_duplicate_rows": duplicate_rows, "negative_adjustment_rows": negative_rows,
                      "replayed": False}
            con.execute("BEGIN TRANSACTION")
            in_transaction = True
            for table in ("silver_trips", "quarantine", "gold_hourly"):
                con.execute(f"DELETE FROM {table} WHERE source_month=?", [month])
            _hook(failure_hook, "after_delete")
            con.execute("INSERT INTO silver_trips SELECT ?,?,* FROM accepted", [month, sha])
            con.execute("INSERT INTO quarantine SELECT ?,?,source_row,reasons FROM staged WHERE len(reasons)>0", [month, sha])
            con.execute("""
              INSERT INTO gold_hourly
              SELECT source_month, date_trunc('hour',pickup_at), pickup_zone_id, negative_adjustment,
                COUNT(*), SUM(trip_distance), SUM(date_diff('second',pickup_at,dropoff_at)),
                SUM(fare_amount), SUM(total_amount), COUNT(*) FILTER(WHERE candidate_duplicate)
              FROM silver_trips WHERE source_month=?
              GROUP BY source_month, date_trunc('hour',pickup_at), pickup_zone_id, negative_adjustment
            """, [month])
            if previous is None:
                con.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
                    month, sha, metadata["source_url"], metadata["captured_at"], metadata["file_bytes"],
                    metadata["raw_relative_path"], schema_json, CONTRACT_VERSION, raw_rows, accepted,
                    quarantined, duplicate_rows, negative_rows, _utc(), run_id,
                ])
            con.execute("DELETE FROM current_sources WHERE source_month=?", [month])
            con.execute("INSERT INTO current_sources VALUES (?,?)", [month, sha])
            _hook(failure_hook, "before_commit")
            con.execute("COMMIT")
            in_transaction = False
            committed = True
        con.execute("UPDATE run_audit SET status='committed',finished_at=?,replayed=? WHERE run_id=?",
                    [_utc(), result["replayed"], run_id])
    except Exception as exc:
        if in_transaction:
            con.execute("ROLLBACK")
        con.execute("UPDATE run_audit SET status=?,finished_at=?,error_type=? WHERE run_id=?",
                    ["committed_export_failed" if committed else "failed", _utc(), type(exc).__name__, run_id])
        LOG.error("ingest_failed run_id=%s month=%s error_type=%s", run_id, month, type(exc).__name__)
        raise
    finally:
        con.close()
    # Independent output publication follows DB commit. Never claim that database
    # and filesystem artifacts participate in the same distributed transaction.
    try:
        exported = report(data_dir)
        result["report_generation"] = exported["generation"]
    except Exception as exc:
        with _connect(data_dir) as audit:
            audit.execute("UPDATE run_audit SET status='committed_export_failed',error_type=? WHERE run_id=?",
                          [type(exc).__name__, run_id])
        raise
    LOG.info("ingest_committed run_id=%s month=%s rows=%s replayed=%s", run_id, month, result["raw_rows"], result["replayed"])
    return result


def _rows(con, query: str) -> list[dict]:
    cursor = con.execute(query)
    names = [x[0] for x in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _write_csv(con, query: str, path: Path):
    cursor = con.execute(query)
    with path.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow([x[0] for x in cursor.description])
        while rows := cursor.fetchmany(10_000):
            writer.writerows(rows)


def report(data_dir) -> dict:
    """Publish coherent aggregated CSV/JSON from a single DuckDB read transaction."""
    data_dir = Path(data_dir)
    generation = str(uuid.uuid4())
    output = data_dir / "reports" / "generations" / generation
    output.mkdir(parents=True)
    con = _connect(data_dir, read_only=True)
    try:
        con.execute("BEGIN TRANSACTION")
        sources = _rows(con, """
          SELECT source_month,source_sha256,source_url,captured_at,file_bytes,raw_rows,
            accepted_rows,quarantined_rows,candidate_duplicate_rows,negative_adjustment_rows
          FROM current_source_versions ORDER BY source_month
        """)
        totals = {name: sum(source[name] for source in sources) for name in (
            "raw_rows", "accepted_rows", "quarantined_rows", "candidate_duplicate_rows", "negative_adjustment_rows"
        )}
        metrics = _rows(con, """
          SELECT source_month,negative_adjustment, SUM(trip_count) AS trip_count,
            SUM(fare_amount_sum) AS fare_amount_sum, SUM(total_amount_sum) AS total_amount_sum,
            SUM(trip_distance_sum) AS trip_distance_sum,
            SUM(duration_seconds_sum)::DOUBLE / NULLIF(SUM(trip_count),0) AS mean_duration_seconds
          FROM gold_hourly GROUP BY source_month,negative_adjustment
          ORDER BY source_month,negative_adjustment
        """)
        result = {"generation": generation, "generated_at": _utc(), "contract_version": CONTRACT_VERSION,
                  "execution": "local DuckDB; Azure tests not executed", "totals": totals,
                  "sources": sources, "monthly_metrics": metrics,
                  "quality_reasons": _rows(con, "SELECT * FROM quality_summary ORDER BY source_month,reason")}
        _write_csv(con, "SELECT * FROM gold_hourly ORDER BY source_month,pickup_hour,pickup_zone_id,negative_adjustment", output / "hourly.csv")
        _write_csv(con, "SELECT * FROM quality_summary ORDER BY source_month,reason", output / "quality.csv")
        (output / "report.json").write_text(_json_text(result))
        con.execute("COMMIT")
        # This is the sole public pointer. An interrupted generation is harmless;
        # retain or remove unreferenced generations under the documented runbook.
        temporary = data_dir / "reports" / ".latest.json.tmp"
        temporary.write_text(_json_text({"generation": generation, "report": f"generations/{generation}/report.json"}))
        os.replace(temporary, data_dir / "reports" / "latest.json")
        # CLI/API are JSON-safe without Decimal/timestamp-specific caller handling.
        return json.loads(_json_text(result))
    finally:
        con.close()


def fetch(month: str, data_dir) -> dict:
    """Bounded, credential-free fetch of one Jan–Mar 2024 TLC source file.

    45-second socket timeouts, three attempts and a byte bound are deliberately
    described separately: the socket timeout is not an end-to-end deadline.
    """
    _month(month)
    if month not in ALLOWED_FETCH_MONTHS:
        raise ContractError("fetch scope is January–March 2024; ingest supports reviewed local months")
    url = f"{SOURCE_BASE}/yellow_tripdata_{month}.parquet"
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(3):
        fd, path = tempfile.mkstemp(prefix=".download-", suffix=".parquet", dir=data_dir)
        temp = Path(path)
        try:
            request = Request(url, headers={"User-Agent": "urban-mobility-lakehouse/0.1 (public portfolio research)"})
            with os.fdopen(fd, "wb") as target, urlopen(request, timeout=45) as response:
                declared = response.headers.get("Content-Length")
                expected = int(declared) if declared is not None else None
                if expected is not None and expected > MAX_SOURCE_BYTES:
                    raise ContractError("remote source exceeds byte limit")
                total = 0
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > MAX_SOURCE_BYTES:
                        raise ContractError("remote source exceeds byte limit")
                    target.write(chunk)
                if expected is not None and total != expected:
                    raise ContractError("download length differs from Content-Length")
                target.flush()
                os.fsync(target.fileno())
            return ingest(temp, month, data_dir, source_url=url)
        except HTTPError as exc:
            last_error = exc
            exc.close()
            if exc.code not in (408, 429, 500, 502, 503, 504):
                raise
        except (URLError, TimeoutError, ConnectionError) as exc:
            last_error = exc
        finally:
            temp.unlink(missing_ok=True)
        if attempt < 2:
            time.sleep(2 ** attempt)
    raise RuntimeError("source download failed after three attempts") from last_error
