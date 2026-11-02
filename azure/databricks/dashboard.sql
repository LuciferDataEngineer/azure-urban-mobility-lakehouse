-- Save each SELECT as a Databricks SQL dashboard dataset after approved deployment.
-- This file is implemented SQL; no hosted dashboard/warehouse exists yet.
-- 1: Monthly quality and conservation (table + reject_fraction bar).
SELECT *, source_rows - regular_rows - adjustment_rows - quarantined_rows AS unexplained_rows
FROM mobility.yellow.gold_quality ORDER BY source_month;
-- 2: Hourly demand (line). Accepted rows are records, not distinct people.
SELECT pickup_hour, sum(regular_rows) AS regular_rows, sum(adjustment_rows) AS adjustments
FROM mobility.yellow.gold_hourly_zone GROUP BY pickup_hour ORDER BY pickup_hour;
-- 3: Zone activity ranking; interpret sentinel IDs 264/265 as unknown/unmapped.
SELECT pickup_zone_id, sum(regular_rows) AS regular_rows,
       sum(regular_total_usd) AS regular_total_usd, sum(adjustment_total_usd) AS adjustments_usd
FROM mobility.yellow.gold_hourly_zone GROUP BY pickup_zone_id ORDER BY regular_rows DESC LIMIT 20;
-- 4: Rejection reasons (one source row may have more than one reason).
SELECT source_month, reason, count(*) AS reason_occurrences
FROM mobility.yellow.quarantine LATERAL VIEW explode(reject_reasons) r AS reason
GROUP BY source_month, reason ORDER BY source_month, reason_occurrences DESC;
-- 5: Candidate duplicate investigation, not deletion advice.
SELECT * FROM mobility.yellow.candidate_duplicates ORDER BY occurrences DESC LIMIT 50;
-- 6: History / corrected versions retained in Bronze.
SELECT source_month, source_sha256, contract_version, count(*) AS source_rows,
       min(captured_at_utc) AS normalized_at_utc
FROM mobility.yellow.bronze_versions GROUP BY source_month, source_sha256, contract_version
ORDER BY source_month, normalized_at_utc;
