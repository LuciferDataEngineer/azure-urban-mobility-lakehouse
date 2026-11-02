from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from mobility_lakehouse import pipeline
from scripts.generate_fixture import SCHEMA, sample_rows, trip, write_fixture


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / "data"
        self.serial = 0

    def source(self, rows=None, schema=SCHEMA):
        self.serial += 1
        return write_fixture(self.root / f"source-{self.serial}.parquet", rows, schema=schema)

    def ingest(self, source=None, month="2024-01", **kwargs):
        return pipeline.ingest(source or self.source(), month, self.data,
                               source_url="https://example.org/synthetic.parquet", **kwargs)

    def query(self, sql, params=None):
        with duckdb.connect(str(self.data / "warehouse.duckdb"), read_only=True) as conn:
            return conn.execute(sql, params or []).fetchall()

    def business_state(self):
        result = {}
        with duckdb.connect(str(self.data / "warehouse.duckdb"), read_only=True) as conn:
            for table in ("silver_trips", "quarantine", "sources", "current_sources", "gold_hourly"):
                result[table] = sorted(map(repr, conn.execute(f"SELECT * FROM {table}").fetchall()))
        return result

    def test_fixture_bytes_are_reproducible(self):
        self.assertEqual(self.source().read_bytes(), self.source().read_bytes())

    def test_conservation_duplicates_and_adjustments(self):
        result = self.ingest()
        self.assertEqual(result["raw_rows"], 6)
        self.assertEqual(result["accepted_rows"], 4)
        self.assertEqual(result["quarantined_rows"], 2)
        self.assertEqual(result["candidate_duplicate_rows"], 1)
        self.assertEqual(result["negative_adjustment_rows"], 1)
        self.assertEqual(self.query("SELECT COUNT(*), SUM(fare_amount), SUM(total_amount), SUM(trip_distance) FROM silver_trips"),
                         [(4, Decimal("22.50"), Decimal("26.60"), 5.0)])
        # A duplicate candidate remains a separate record; financial corrections remain signed.
        self.assertEqual(self.query("SELECT source_row FROM silver_trips ORDER BY source_row"), [(0,), (1,), (2,), (3,)])
        self.assertEqual(self.query("SELECT SUM(total_amount) FROM silver_trips WHERE NOT negative_adjustment"), [(Decimal("32.60"),)])
        self.assertEqual(self.query("SELECT SUM(total_amount) FROM silver_trips WHERE negative_adjustment"), [(Decimal("-6.00"),)])
        self.assertEqual(self.query("SELECT source_row FROM quarantine ORDER BY source_row"), [(4,), (5,)])

    def test_replay_does_not_change_business_state(self):
        source = self.source()
        first = self.ingest(source)
        before = self.business_state()
        second = self.ingest(source)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(before, self.business_state())

    def test_source_identity_is_sha_and_physical_row_not_business_columns(self):
        source = self.source([trip(), trip()])
        result = self.ingest(source)
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        self.assertEqual(result["source_sha256"], digest)
        self.assertEqual(self.query("SELECT source_sha256, source_row FROM silver_trips ORDER BY source_row"), [(digest, 0), (digest, 1)])

    def test_corrected_month_replaces_only_that_slice(self):
        jan = self.source([trip()])
        feb = self.source([trip(tpep_pickup_datetime=datetime(2024, 2, 2, 10), tpep_dropoff_datetime=datetime(2024, 2, 2, 10, 15), total_amount=30)])
        self.ingest(jan)
        self.ingest(feb, month="2024-02")
        before_feb = self.query("SELECT * FROM silver_trips WHERE source_month='2024-02'")
        corrected = self.source([trip(total_amount=15), trip(tpep_pickup_datetime=datetime(2024, 1, 3, 10), tpep_dropoff_datetime=datetime(2024, 1, 3, 10, 15), total_amount=20)])
        result = self.ingest(corrected)
        self.assertFalse(result["replayed"])
        self.assertEqual(self.query("SELECT source_month, COUNT(*), SUM(total_amount) FROM silver_trips GROUP BY source_month ORDER BY source_month"),
                         [("2024-01", 2, Decimal("35.00")), ("2024-02", 1, Decimal("30.00"))])
        self.assertEqual(self.query("SELECT * FROM silver_trips WHERE source_month='2024-02'"), before_feb)
        self.assertEqual(self.query("SELECT COUNT(*) FROM sources"), [(3,)])

    def test_previous_source_version_cannot_silently_regress_current_month(self):
        original = self.source([trip(total_amount=12)])
        corrected = self.source([trip(total_amount=20)])
        self.ingest(original)
        self.ingest(corrected)
        before = self.business_state()
        with self.assertRaises(pipeline.ContractError):
            self.ingest(original)
        self.assertEqual(before, self.business_state())
        self.ingest(original, allow_revert=True)
        self.assertEqual(self.query("SELECT SUM(total_amount) FROM silver_trips"), [(Decimal("12.00"),)])

    def test_missing_required_column_fails_without_changing_existing_month(self):
        self.ingest()
        before = self.business_state()
        table = pa.Table.from_pylist([trip()], schema=SCHEMA).drop(["fare_amount"])
        source = self.root / "missing.parquet"
        pq.write_table(table, source)
        with self.assertRaises(pipeline.ContractError):
            self.ingest(source)
        self.assertEqual(before, self.business_state())

    def test_wrong_timestamp_type_is_controlled_schema_error(self):
        schema = pa.schema([(field.name, pa.string() if field.name == "tpep_pickup_datetime" else field.type) for field in SCHEMA])
        source = self.source([trip(tpep_pickup_datetime="not-a-timestamp")], schema=schema)
        with self.assertRaises(pipeline.ContractError):
            self.ingest(source)

    def test_timezone_aware_timestamp_is_not_silently_reinterpreted(self):
        table = pa.Table.from_pylist([trip()], schema=SCHEMA)
        idx = table.schema.get_field_index("tpep_pickup_datetime")
        table = table.set_column(idx, "tpep_pickup_datetime", table.column(idx).cast(pa.timestamp("us", tz="UTC")))
        source = self.root / "aware.parquet"
        pq.write_table(table, source)
        with self.assertRaises(pipeline.ContractError):
            self.ingest(source)

    def test_additive_column_preserved_in_schema_provenance(self):
        table = pa.Table.from_pylist([trip()], schema=SCHEMA).append_column("new_vendor_field", pa.array(["example"]))
        source = self.root / "additive.parquet"
        pq.write_table(table, source)
        result = self.ingest(source)
        self.assertEqual(result["accepted_rows"], 1)
        self.assertIn("new_vendor_field", repr(self.query("SELECT * FROM sources")))

    def test_invalid_records_quarantine_with_reasons_and_conserve_input(self):
        rows = [
            trip(),
            trip(tpep_pickup_datetime=None),
            trip(tpep_dropoff_datetime=None),
            trip(tpep_pickup_datetime=datetime(2024, 2, 1), tpep_dropoff_datetime=datetime(2024, 2, 1, 0, 15)),
            trip(tpep_dropoff_datetime=datetime(2024, 1, 1, 10, 0)),
            trip(PULocationID=0),
            trip(DOLocationID=266),
            trip(trip_distance=-1),
            trip(trip_distance=float("inf")),
            trip(fare_amount=float("nan")),
            trip(total_amount=None),
            trip(total_amount=1e30),
        ]
        result = self.ingest(self.source(rows))
        self.assertEqual((result["raw_rows"], result["accepted_rows"], result["quarantined_rows"]), (12, 1, 11))
        rejected = self.query("SELECT source_row, reasons FROM quarantine ORDER BY source_row")
        self.assertEqual([r[0] for r in rejected], list(range(1, 12)))
        self.assertTrue(all(r[1] for r in rejected))

    def test_fractional_zone_id_is_not_truncated_into_valid_zone(self):
        schema = pa.schema([(field.name, pa.float64() if field.name == "PULocationID" else field.type) for field in SCHEMA])
        result = self.ingest(self.source([trip(PULocationID=1.5)], schema=schema))
        self.assertEqual((result["accepted_rows"], result["quarantined_rows"]), (0, 1))

    def test_amount_rounding_is_signed_cents(self):
        # 2.625 is exactly representable as a binary float, so this checks the stated
        # half-away-from-zero rounding without assuming decimal input precision.
        result = self.ingest(self.source([trip(fare_amount=2.625, total_amount=2.625), trip(fare_amount=-2.625, total_amount=-2.625)]))
        self.assertEqual(result["accepted_rows"], 2)
        self.assertEqual(self.query("SELECT fare_amount,total_amount FROM silver_trips ORDER BY source_row"),
                         [(Decimal("2.63"), Decimal("2.63")), (Decimal("-2.63"), Decimal("-2.63"))])

    def test_trip_crossing_month_boundary_belongs_to_pickup_month(self):
        row = trip(tpep_pickup_datetime=datetime(2024, 1, 31, 23, 55),
                   tpep_dropoff_datetime=datetime(2024, 2, 1, 0, 10))
        result = self.ingest(self.source([row]))
        self.assertEqual((result["accepted_rows"], result["quarantined_rows"]), (1, 0))

    def test_optional_provider_columns_can_be_absent(self):
        table = pa.Table.from_pylist([trip()], schema=SCHEMA).drop(["VendorID", "passenger_count", "payment_type"])
        source = self.root / "minimal-contract.parquet"
        pq.write_table(table, source)
        result = self.ingest(source)
        self.assertEqual(result["accepted_rows"], 1)
        self.assertEqual(self.query("SELECT vendor_id,passenger_count,payment_type FROM silver_trips"), [(None, None, None)])

    def test_zero_distance_is_kept_without_inventing_a_speed(self):
        result = self.ingest(self.source([trip(trip_distance=0)]))
        self.assertEqual(result["accepted_rows"], 1)
        self.assertEqual(self.query("SELECT trip_distance FROM silver_trips"), [(0.0,)])

    def test_transaction_failure_after_delete_retains_old_month(self):
        original = self.source([trip(total_amount=12)])
        self.ingest(original)
        before = self.business_state()
        corrected = self.source([trip(total_amount=99)])
        seen = []
        def fail(phase):
            seen.append(phase)
            if phase == "after_delete":
                raise RuntimeError("injected transform failure after DELETE")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.ingest(corrected, failure_hook=fail)
        self.assertIn("after_delete", seen)
        self.assertEqual(before, self.business_state())
        # Recovery is replay of retained bytes, not manual business-table repair.
        self.ingest(corrected)
        self.assertEqual(self.query("SELECT total_amount FROM silver_trips"), [(Decimal("99.00"),)])

    def test_transaction_failure_before_commit_rolls_back_new_facts_and_pointer(self):
        self.ingest(self.source([trip(total_amount=12)]))
        before = self.business_state()
        def fail(phase):
            if phase == "before_commit":
                raise RuntimeError("injected precommit failure")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.ingest(self.source([trip(total_amount=99)]), failure_hook=fail)
        self.assertEqual(before, self.business_state())

    def test_raw_hash_archive_survives_transform_failure(self):
        source = self.source([trip(total_amount=55)])
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        def fail(phase):
            if phase == "after_raw":
                raise RuntimeError("injected after archive")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.ingest(source, failure_hook=fail)
        archived = [p for p in self.data.rglob("*.parquet") if hashlib.sha256(p.read_bytes()).hexdigest() == digest]
        self.assertTrue(archived)
        self.assertEqual(self.ingest(archived[0])["accepted_rows"], 1)

    def test_truncated_parquet_does_not_replace_committed_data(self):
        self.ingest()
        before = self.business_state()
        bad = self.root / "truncated.parquet"
        bad.write_bytes(b"PAR1bad")
        with self.assertRaises((pipeline.ContractError, pa.ArrowInvalid)):
            self.ingest(bad)
        self.assertEqual(before, self.business_state())

    def test_hourly_serving_metrics_reconcile_with_signed_fact_groups(self):
        self.ingest()
        hourly = self.query("SELECT pickup_hour, pickup_zone_id, negative_adjustment, trip_count, trip_distance_sum, duration_seconds_sum, fare_amount_sum, total_amount_sum, candidate_duplicate_rows FROM gold_hourly ORDER BY pickup_hour,pickup_zone_id,negative_adjustment")
        self.assertEqual(hourly, [
            (datetime(2024, 1, 1, 10), 1, False, 2, 4.0, 1800, Decimal("20.50"), Decimal("24.60"), 1),
            (datetime(2024, 1, 1, 10), 1, True, 1, 1.0, 900, Decimal("-5.00"), Decimal("-6.00"), 0),
            (datetime(2024, 1, 2, 12), 2, False, 1, 0.0, 600, Decimal("7.00"), Decimal("8.00"), 0),
        ])
        report = pipeline.report(self.data)
        self.assertEqual(report["totals"]["accepted_rows"], 4)
        ordinary, adjustment = report["monthly_metrics"]
        self.assertEqual(ordinary["trip_count"], 3)
        self.assertEqual(Decimal(ordinary["total_amount_sum"]), Decimal("32.60"))
        self.assertEqual(ordinary["mean_duration_seconds"], 800)
        self.assertEqual(adjustment["trip_count"], 1)
        self.assertEqual(Decimal(adjustment["total_amount_sum"]), Decimal("-6.00"))

    def test_empty_month_cannot_silently_delete_published_slice(self):
        self.ingest()
        before = self.business_state()
        with self.assertRaisesRegex(pipeline.ContractError, "empty monthly"):
            self.ingest(self.source([]))
        self.assertEqual(before, self.business_state())

    def test_corrupted_retained_object_is_detected_before_replay(self):
        source = self.source([trip()])
        self.ingest(source)
        before = self.business_state()
        archived = next((self.data / "raw").rglob("source.parquet"))
        original_bytes = archived.read_bytes()
        archived.write_bytes(b"corrupted retained content")
        with self.assertRaisesRegex(pipeline.ContractError, "checksum mismatch"):
            self.ingest(source)
        self.assertEqual(before, self.business_state())
        archived.write_bytes(original_bytes)
        self.assertTrue(self.ingest(source)["replayed"])

    def test_invalid_month_is_a_controlled_input_error(self):
        for value in ("2024-13", "2024-00", "2024-1", "../../2024-01"):
            with self.subTest(month=value):
                with self.assertRaises(pipeline.ContractError):
                    self.ingest(month=value)

    def test_finite_records_cannot_commit_a_nonfinite_aggregate(self):
        self.ingest()
        before = self.business_state()
        source = self.source([trip(trip_distance=1e308), trip(trip_distance=1e308)])
        with self.assertRaisesRegex(pipeline.ContractError, "aggregate"):
            self.ingest(source)
        self.assertEqual(before, self.business_state())

    def test_fetch_retains_source_provenance_and_declares_socket_timeout(self):
        payload = self.source([trip()]).read_bytes()
        response = io.BytesIO(payload)
        response.headers = {"Content-Length": str(len(payload))}
        with patch.object(pipeline, "urlopen", return_value=response) as request:
            result = pipeline.fetch("2024-01", self.data)
        self.assertEqual(result["accepted_rows"], 1)
        call_args = request.call_args
        self.assertEqual(call_args.kwargs["timeout"], 45)
        expected_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        self.assertEqual(call_args.args[0].full_url, expected_url)
        self.assertEqual(self.query("SELECT source_url FROM sources"), [(expected_url,)])
        self.assertFalse(list(self.data.glob(".download-*")))

    def test_fetch_rejects_oversized_declared_length_before_ingestion(self):
        response = io.BytesIO(b"not required")
        response.headers = {"Content-Length": "101"}
        with patch.object(pipeline, "urlopen", return_value=response), patch.object(pipeline, "MAX_SOURCE_BYTES", 100), patch.object(pipeline, "ingest") as ingestion:
            with self.assertRaisesRegex(pipeline.ContractError, "byte limit"):
                pipeline.fetch("2024-01", self.data)
        ingestion.assert_not_called()
        self.assertFalse(list(self.data.glob(".download-*")))

    def test_fetch_rejects_oversized_stream_without_declared_length(self):
        response = io.BytesIO(b"x" * 101)
        response.headers = {}
        with patch.object(pipeline, "urlopen", return_value=response), patch.object(pipeline, "MAX_SOURCE_BYTES", 100), patch.object(pipeline, "ingest") as ingestion:
            with self.assertRaisesRegex(pipeline.ContractError, "byte limit"):
                pipeline.fetch("2024-01", self.data)
        ingestion.assert_not_called()
        self.assertFalse(list(self.data.glob(".download-*")))

    def test_fetch_rejects_incomplete_content_length(self):
        payload = self.source([trip()]).read_bytes()
        response = io.BytesIO(payload)
        response.headers = {"Content-Length": str(len(payload) + 1)}
        with patch.object(pipeline, "urlopen", return_value=response), patch.object(pipeline, "ingest") as ingestion:
            with self.assertRaisesRegex(pipeline.ContractError, "Content-Length"):
                pipeline.fetch("2024-01", self.data)
        ingestion.assert_not_called()
        self.assertFalse(list(self.data.glob(".download-*")))

    def test_fetch_transient_failure_retries_then_succeeds(self):
        payload = self.source([trip()]).read_bytes()
        response = io.BytesIO(payload)
        response.headers = {"Content-Length": str(len(payload))}
        error = HTTPError("https://example.org/source", 503, "temporary", {}, None)
        with patch.object(pipeline, "urlopen", side_effect=[error, response]) as request, patch.object(pipeline.time, "sleep") as sleeper:
            self.assertEqual(pipeline.fetch("2024-01", self.data)["accepted_rows"], 1)
        self.assertEqual(request.call_count, 2)
        sleeper.assert_called_once_with(1)
        self.assertTrue(error.closed)

    def test_fetch_permanent_http_error_is_closed_without_retry(self):
        error = HTTPError("https://example.org/source", 404, "missing", {}, None)
        with patch.object(pipeline, "urlopen", side_effect=error) as request, patch.object(pipeline.time, "sleep") as sleeper:
            with self.assertRaises(HTTPError):
                pipeline.fetch("2024-01", self.data)
        self.assertEqual(request.call_count, 1)
        self.assertTrue(error.closed)
        sleeper.assert_not_called()

    def test_fetch_stops_after_three_transport_failures(self):
        with patch.object(pipeline, "urlopen", side_effect=URLError("injected transport failure")) as request, patch.object(pipeline.time, "sleep") as sleeper:
            with self.assertRaisesRegex(RuntimeError, "three attempts"):
                pipeline.fetch("2024-01", self.data)
        self.assertEqual(request.call_count, 3)
        self.assertEqual([c.args[0] for c in sleeper.call_args_list], [1, 2])
        self.assertFalse(list(self.data.glob(".download-*")))

    def test_fetch_scope_is_bounded_before_http_request(self):
        with patch.object(pipeline, "urlopen") as request:
            with self.assertRaises(pipeline.ContractError):
                pipeline.fetch("2026-01", self.data)
        request.assert_not_called()

    def test_export_failure_keeps_prior_pointer_then_replay_recovers(self):
        self.ingest(self.source([trip(total_amount=12)]))
        pointer_candidates = list(self.data.rglob("latest.json"))
        self.assertEqual(len(pointer_candidates), 1)
        pointer = pointer_candidates[0]
        before = pointer.read_bytes()
        corrected = self.source([trip(total_amount=99)])
        replace = pipeline.os.replace
        def fail_pointer(src, dst):
            if Path(dst).name == "latest.json":
                raise OSError("injected export pointer failure")
            return replace(src, dst)
        with patch.object(pipeline.os, "replace", side_effect=fail_pointer):
            with self.assertRaisesRegex(OSError, "injected"):
                self.ingest(corrected)
        self.assertEqual(pointer.read_bytes(), before)
        self.assertEqual(self.query("SELECT total_amount FROM silver_trips"), [(Decimal("99.00"),)])
        result = self.ingest(corrected)
        self.assertTrue(result["replayed"])
        self.assertNotEqual(pointer.read_bytes(), before)
        json.loads(pointer.read_text())


if __name__ == "__main__":
    unittest.main()
