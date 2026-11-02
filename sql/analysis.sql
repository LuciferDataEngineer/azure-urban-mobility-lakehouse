-- Execute with DuckDB against data/warehouse.duckdb. The default library never
-- downloads SQL extensions or reaches remote storage; queries use retained data.

-- 1. Source lineage and accepted + quarantine conservation by active month.
SELECT source_month, source_sha256, raw_rows, accepted_rows, quarantined_rows,
       raw_rows = accepted_rows + quarantined_rows AS conserves_rows
FROM current_source_versions ORDER BY source_month;

-- 2. Gross ordinary-record totals and separately retained signed adjustments.
-- total_amount is not profit, driver take-home pay or cash tip coverage.
SELECT source_month, negative_adjustment, SUM(trip_count) AS records,
       SUM(fare_amount_sum) AS signed_fares_usd,
       SUM(total_amount_sum) AS signed_total_amount_usd
FROM gold_hourly GROUP BY source_month, negative_adjustment
ORDER BY source_month, negative_adjustment;

-- 3. Busiest observed pickup zone IDs, including duplicates (never auto-dropped).
SELECT pickup_zone_id, SUM(trip_count) AS records,
       SUM(total_amount_sum) / NULLIF(SUM(trip_count), 0) AS mean_total_amount_usd
FROM gold_hourly WHERE NOT negative_adjustment
GROUP BY pickup_zone_id ORDER BY records DESC, pickup_zone_id LIMIT 20;

-- 4. Hour-of-day profile in recorded NYC civil time, not UTC/event-time intervals.
SELECT EXTRACT(hour FROM pickup_hour) AS civil_hour, SUM(trip_count) AS records,
       SUM(duration_seconds_sum)::DOUBLE / NULLIF(SUM(trip_count), 0) AS mean_civil_duration_seconds
FROM gold_hourly WHERE NOT negative_adjustment
GROUP BY civil_hour ORDER BY civil_hour;

-- 5. Multiple reasons can refer to the same quarantined row; do not sum as records.
SELECT * FROM quality_summary ORDER BY source_month, row_count DESC, reason;

-- 6. Compare retained versions without pretending arrival order is publication order.
SELECT source_month, source_sha256, captured_at, accepted_rows, quarantined_rows,
       source_sha256 = (SELECT c.source_sha256 FROM current_sources c
                       WHERE c.source_month = sources.source_month) AS is_current
FROM sources ORDER BY source_month, captured_at, source_sha256;

-- 7. Operational runs; timestamps and replay audits are not business KPIs.
SELECT status, COUNT(*) AS attempts, COUNT(*) FILTER (WHERE replayed) AS replays
FROM run_audit GROUP BY status ORDER BY status;

-- 8. Candidate duplicate density is an observability metric, not proof of duplicates.
SELECT source_month, SUM(candidate_duplicate_rows) AS candidate_duplicate_rows,
       SUM(candidate_duplicate_rows)::DOUBLE / NULLIF(SUM(trip_count), 0) AS candidate_fraction
FROM gold_hourly GROUP BY source_month ORDER BY source_month;
