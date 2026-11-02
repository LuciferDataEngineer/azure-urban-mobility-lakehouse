-- Install after the first successful cloud load; this is not part of a commit.
-- All serving views share current_rows, so corrected source-month publication
-- is ONE Delta transaction. Bronze keeps all source versions and row payloads.
CREATE OR REPLACE VIEW mobility.yellow.silver_trips AS
SELECT source_month, source_sha256, source_row, pickup_at, dropoff_at,
       pickup_zone_id, dropoff_zone_id, trip_distance, fare_amount,
       total_amount, row_status, vendor_id, passenger_count, payment_type, negative_adjustment,
       row_number() OVER (PARTITION BY source_month, pickup_at, dropoff_at,
         pickup_zone_id, dropoff_zone_id, trip_distance, fare_amount, total_amount,
         vendor_id, passenger_count, payment_type, negative_adjustment
         ORDER BY source_row) > 1 AS candidate_duplicate
FROM mobility.yellow.current_rows WHERE row_status != 'quarantined';
CREATE OR REPLACE VIEW mobility.yellow.quarantine AS
SELECT source_month, source_sha256, source_row, reject_reasons, raw_payload
FROM mobility.yellow.current_rows WHERE row_status = 'quarantined';
CREATE OR REPLACE VIEW mobility.yellow.gold_hourly_zone AS
SELECT source_month, date_trunc('hour', pickup_at) AS pickup_hour,
       pickup_zone_id,
       count(*) AS accepted_rows,
       count_if(candidate_duplicate) AS candidate_duplicate_rows,
       count_if(row_status = 'regular') AS regular_rows,
       count_if(row_status = 'adjustment') AS adjustment_rows,
       sum(CASE WHEN row_status = 'regular' THEN total_amount ELSE 0 END) AS regular_total_usd,
       sum(CASE WHEN row_status = 'adjustment' THEN total_amount ELSE 0 END) AS adjustment_total_usd,
       sum(total_amount) AS signed_total_usd,
       avg(CASE WHEN row_status = 'regular' THEN trip_distance END) AS mean_regular_miles
FROM mobility.yellow.silver_trips
GROUP BY source_month, date_trunc('hour', pickup_at), pickup_zone_id;
CREATE OR REPLACE VIEW mobility.yellow.gold_quality AS
SELECT source_month, source_sha256, contract_version, count(*) AS source_rows,
       count_if(row_status = 'regular') AS regular_rows,
       count_if(row_status = 'adjustment') AS adjustment_rows,
       count_if(row_status = 'quarantined') AS quarantined_rows,
       count_if(row_status = 'quarantined') / count(*) AS reject_fraction,
       max(captured_at_utc) AS captured_at_utc
FROM mobility.yellow.current_rows GROUP BY source_month, source_sha256, contract_version;
-- Exact parsed business-field matches are CANDIDATE duplicates, not a trip key.
-- All records remain in totals. Optional vendor/payment/passenger fields and
-- raw-sign adjustment classification participate in candidate matching.
CREATE OR REPLACE VIEW mobility.yellow.candidate_duplicates AS
SELECT source_month, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
       trip_distance, fare_amount, total_amount, vendor_id, passenger_count, payment_type,
       negative_adjustment, count(*) AS occurrences, count(*) - 1 AS candidate_duplicate_rows
FROM mobility.yellow.silver_trips
GROUP BY source_month, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
         trip_distance, fare_amount, total_amount, vendor_id, passenger_count, payment_type,
         negative_adjustment HAVING count(*) > 1;
