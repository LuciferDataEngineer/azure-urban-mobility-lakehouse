# Observed validation — 3 November 2026 Melbourne

**Local execution only. No Azure deployment or Spark/Delta runtime tests have run.** Results below were measured, not target performance numbers. The live data cover January–March 2024; the retrieval date is November 2026. The clock is UTC in machine evidence, which shows 2 November.

## Deterministic checks

Environment: Python **3.14.7**, DuckDB **2.0.0**, PyArrow **26.0.0**, macOS ARM64. The package was installed from `pyproject.toml` before testing, rather than relying on an accidental import from `src`.

- **34 local pipeline tests passed**, with `ResourceWarning` treated as an error. Covered identical replays, corrected monthly replacement, retained duplicates, adjustments, decimal rounding, schema/type rejection, row-level quarantine, physical-row identity, corruption detection, historical-version guard, empty source rejection, optional columns, month-boundary trips and exact fixture Gold aggregates.
- **1 cross-implementation normalization test passed**, comparing 16 synthetic/edge records through local SQL and the pure Python cloud row normalizer. It checks accepted values and rejection reasons; it does not execute Spark, Delta, cloud I/O or permissions.
- **8 pure Python cloud-contract tests passed**; these are also local unit tests, including additive date/time payload serialization.
- Fault injection after a transaction's DELETE and immediately before COMMIT preserved the prior business state. A post-commit report-pointer failure retained the previous report; replay regenerated it.
- Bounded HTTP mocks verified scope, declared and streamed byte caps, incomplete response rejection, timeout arguments, retry limits, transient recovery and error-response cleanup.
- The six-row synthetic CLI demo and all eight SQL examples plus zone enrichment ran successfully. Its four accepted rows total **USD 26.60** in signed recorded charges; these are invented fixture numbers.

Independent review exposed an aggregate-distance overflow and an HTTP error-response resource leak. Both were fixed and covered by regression tests before publication. A first live verification helper used floating accumulation for a large row-hash sum; it was corrected to exact `DECIMAL(38,0)` accumulation and rerun. This was a verification-helper defect, not a source or business-data change.

## Bounded public-source smoke

One complete source file per selected month was fetched from the official linked CloudFront endpoint. January exercised the installed `fetch` command; February and March used bounded public downloads followed by installed `ingest` commands with source URLs. Download bytes were limited; each downloaded file was below 61 MB. No account or token was required.

| Source month | Raw records | Accepted | Quarantined | Retained signed-adjustment records |
|---|---:|---:|---:|---:|
| January 2024 | 2,964,624 | 2,963,736 | 888 | 37,563 |
| February 2024 | 3,007,526 | 3,006,708 | 818 | 40,732 |
| March 2024 | 3,582,628 | 3,581,477 | 1,151 | 58,600 |
| **Total** | **9,554,778** | **9,551,921** | **2,857** | **136,895** |

One candidate duplicate beyond the first identical modeled record was observed, in February. It was retained. The low count does not establish unique real-world rides, because the candidate definition is intentionally narrow.

The 2,857 rejected records comprised 2,801 nonpositive-duration records and 56 records with pickup timestamps outside the declared source month. These categories happened not to overlap in this observed source set; the implementation allows multiple reasons per record. No claim is made about why the source contains these values.

| Input | Bytes | SHA-256 |
|---|---:|---|
| January | 49,961,641 | `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510` |
| February | 50,349,284 | `c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20` |
| March | 60,078,280 | `2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823` |

Additional checks actually run:

1. Rehashed all retained originals and compared Parquet metadata counts with accepted + quarantine counts.
2. Verified every physical row ordinal appears exactly once across the two current row sets, with no missing or duplicate ordinal and the expected source SHA.
3. Reconciled signed decimal fare and total-charge sums from Silver to Gold for every month.
4. Replayed all three retained files and observed `replayed: true`; count/XOR/exact-sum row fingerprints for five business tables were unchanged. These fingerprints are a strong consistency check, not cryptographic equality proof. The small deterministic tests compare full business states exactly.
5. Compared every field of **304,196 hourly CSV rows** and **6 quality CSV rows** with ordered database results.
6. Verified the zone lookup checksum and unique IDs, then checked the join preserved **9,551,921** accepted records with **zero unmatched hourly groups**.

Example analytical result: among ordinary accepted records, zone 161 (Midtown Center in the November 2026 lookup) had 447,728 pickup records over the selected three months. This is a historical descriptive count, not a current ranking or a causal result. All query outputs are in [sql-results-2026-11-03.json](../examples/sql-results-2026-11-03.json).

Machine-readable evidence: [live validation](../examples/live-validation-2026-11-03.json), [aggregate report](../examples/report-2026-11-03.json), [synthetic SQL](../examples/synthetic-sql-results.json). Floating distance/duration aggregates may differ in the last binary digits across query plans; signed monetary totals use exact decimal arithmetic.

## Reproduce

```bash
python -m pip install .
python -W error::ResourceWarning -m unittest discover -s tests -v
python -W error::ResourceWarning -m unittest discover -s azure -p 'test_*.py' -v
python -m compileall -q src azure scripts

# After loading the three public files using README commands:
python scripts/validate_live.py --data-dir data/live
python scripts/run_queries.py --data-dir data/live
```

`validate_live.py` reads retained files and replays them; it does not download. It expects exactly the three selected months. A later upstream file revision can legitimately change hashes and results; retain the new bytes as a separate version and investigate rather than asserting these counts forever.

## Azure evidence boundary

The Bicep foundation and orchestration modules, and development parameter file, compiled locally with Bicep **0.46.1**. The production parameter file deliberately fails the `environment='dev'` guard; production private networking is not implemented. Python cloud adapters passed syntax compilation. Those checks do not validate Azure provider availability, quotas, permissions, Databricks APIs, notebook imports or pipeline expressions at runtime.

**Not run:** Azure deployment/what-if, ADF copy and job execution, Delta atomicity and crash recovery, Unity Catalog and managed-identity access tests, private networking, SQL warehouse/dashboard deployment, live alert delivery, cost measurement, distributed throughput/load tests, cloud backup/restore or production acceptance. No production-ready claim is made. See [AZURE.md](AZURE.md) for the staged acceptance plan and remaining gaps.
