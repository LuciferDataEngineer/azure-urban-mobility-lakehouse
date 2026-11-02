# Contract and data dictionary

Local contract: `tlc-yellow-local-v1`. An existing warehouse with another contract fails closed; there is no automatic migration. SQL DDL and transformations live in `src/mobility_lakehouse/pipeline.py`. The Azure adapter is a separate implementation; its schema is documented with its code and must pass parity tests before cloud release.

## Input

The fetch adapter permits only January–March 2024. Local `ingest` permits reviewed month values from 2000–2100. Limits: 150 MiB compressed file, 20 million rows, 128 columns and 5,000 row groups. Zero-row files cannot silently delete a month. Duplicate/case-colliding field names and reserved Parquet virtual column names fail the contract.

| Source field | Required type | Meaning / treatment |
|---|---|---|
| `tpep_pickup_datetime` | Timezone-naive Arrow timestamp | NYC meter-start civil time; must be finite, non-null and within declared source month |
| `tpep_dropoff_datetime` | Timezone-naive Arrow timestamp | Must be finite, non-null and later than pickup; may cross a month boundary |
| `PULocationID`, `DOLocationID` | Numeric Arrow type | Must be finite whole numbers 1–265, including special/unknown IDs |
| `trip_distance` | Numeric Arrow type | Miles, finite and ≥0; zero is retained |
| `fare_amount`, `total_amount` | Numeric Arrow type | Signed USD; finite, non-null and representable in target decimal |
| `VendorID`, `passenger_count`, `payment_type` | Numeric if present | Optional, retained as nullable doubles; not completeness/domain-validated for KPIs |
| Additional columns | Preserved raw; types recorded | Accepted without assigning new analytical meaning |

Missing required columns, changed timestamp types, timezone-aware timestamps and nonnumeric modeled numeric fields reject the **file**. Bad values in a structurally compatible file quarantine the **row**. There are six reason codes: `invalid_timestamp`, `pickup_outside_source_month`, `nonpositive_duration`, `invalid_location_id`, `invalid_trip_distance`, `invalid_money`. One record can have several reasons. Quarantine counts count records; reason counts may sum to more than the quarantine count. A month in which every row is invalid can commit all rows to quarantine; monitoring must inspect reject rates rather than treating completion alone as data quality.

`DECIMAL(18,2)` amounts are obtained by converting to `DECIMAL(38,9)`, rounding half away from zero to two places, then checking the target range. This normalizes source binary floats; the unmodified source remains retained. The adjustment flag uses the **unrounded** sign (`fare_amount < 0 OR total_amount < 0`), so a tiny negative amount rounded to zero is still an adjustment. No source-total-versus-components reconciliation is asserted: all components are not modeled and cash tips are absent.

## Physical warehouse

| Object | Grain / key | Fields and purpose |
|---|---|---|
| `contract_metadata` | One contract string | Refuses accidental reuse across schema contracts |
| `run_audit` | UUID `run_id` | UTC start/end, declared source month, SHA when available, status, replay flag, error class; attempt audit survives business rollback |
| `sources` | `(source_month, source_sha256)` | HTTPS provenance URL, first local capture UTC, byte count, raw relative path, complete field/type schema JSON, contract, source/accepted/quarantine/duplicate/adjustment counts, first successful commit UTC and run ID |
| `current_sources` | `source_month` | One active source digest per month |
| `silver_trips` | `(source_month, source_sha256, source_row)` | Accepted current physical records: times, zones, miles, signed decimal fare/total, optional source fields, adjustment and candidate-duplicate flags |
| `quarantine` | Same source-row key | Nonempty reason array; original values remain addressable in raw Parquet by physical row |
| `gold_hourly` | `(source_month, pickup_hour, pickup_zone_id, negative_adjustment)` | Materialized record count, miles sum, wall-clock duration-seconds sum, signed fare/total sums, candidate-duplicate count |
| `current_source_versions` | Current source per month | Join of current pointer to retained source metadata |
| `quality_summary` | Month + reason | Record count exhibiting a particular rejection reason |

Source-row ordinals start at zero and refer to the complete physical Parquet file, not a filtered row order. Primary keys enforce identity; an additional `(pickup_zone_id,pickup_at)` index supports selective trip inspection. DuckDB remains a single-machine analytical store. There are no declared cross-table foreign keys; pipeline transactions and validation enforce pointer/slice consistency. Direct ad hoc writes are outside the contract.

UTC audit timestamps are stored as DuckDB `TIMESTAMP` values with an explicit UTC convention; they are different from the source's timezone-naive NYC civil timestamps. Raw manifests use an explicit UTC offset.

## Metrics

| Metric | Definition and caveat |
|---|---|
| Raw record count | Parquet metadata count, reconciled against scanned accepted + quarantined rows |
| Accepted records | Rows satisfying the required quality rules, including possible duplicates and signed adjustments |
| Candidate duplicate records | For each equality group of normalized timestamps, zones, miles, rounded fare/total, optional vendor/passengers/payment and adjustment flag: all occurrences after the first (`n−1`). Not a proven real-world duplicate count; raw-only fields are excluded |
| Ordinary records | Accepted rows with neither raw fare nor raw total negative; this does not mean all are paid fares |
| Signed adjustments | Accepted records with either amount negative; the cause is not inferred as a refund or fraud |
| Signed charges | Sum of rounded recorded `total_amount`; separate ordinary and adjustment groups before any netting |
| Hourly demand | Accepted record count by pickup civil hour and zone, including source ambiguities |
| Mean duration | Sum of `date_diff('second',pickup,dropoff)` / count; wall-clock statistic, not precise elapsed time across DST |
| Candidate fraction | Candidate duplicate count / accepted record count; null if denominator is zero |

No arbitrary maximum duration, distance or fare removes outliers. Inspect unusual distributions before decision-making. The supplementary query runner verifies the zone snapshot digest, its 265 distinct IDs, join cardinality and complete matches before reporting names. The lookup's November 2026 capture date must accompany historical comparisons.

A finite individual distance can still overflow a floating aggregate. Before committing, the pipeline rejects a nonfinite sum of accepted distances; the prior month remains published. This guard is a file-level failure, not a silent row deletion.

## Filesystem contract

`raw/<month>/<sha>/source.parquet` and `manifest.json` are retained original evidence. Replays verify the archived checksum and manifest identity. `reports/generations/<uuid>/` holds a coherent `report.json`, `hourly.csv` and `quality.csv`; `reports/latest.json` points to the latest fully generated report. Monetary JSON values are strings to preserve cents. CSV/JSON exports contain aggregate values and source provenance, not raw individual records.

Prior source versions remain reconstructible from raw files, but only **current** facts/quarantine/Gold are in those tables. Arrival/capture timestamps are not original publisher release dates. The source registry is not a complete sequence of all active-version intervals; run audits show attempts, and explicit reverts are auditable.
