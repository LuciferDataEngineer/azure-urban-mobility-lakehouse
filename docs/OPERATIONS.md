# Local operations and recovery

## Execution boundaries

Run one writer/exporter per data directory, including within one process. DuckDB rejects conflicting writer processes with its file lock. Do not run simultaneous report publications: independent exporters could race on the pointer. Internal query parallelism is limited to two threads with a 1 GB DuckDB memory target and an on-disk spill directory. That limit is not a total process-memory guarantee.

The downloader uses a 150 MiB limit, a 45-second socket timeout and at most three attempts with short exponential backoff for selected transient HTTP/network failures. The timeout is **not** a wall-clock deadline. Partial downloads are removed. A public source may change between requests; manifests capture the exact bytes actually retained. Retrieval errors before `ingest` do not create a database run audit; the CLI exits nonzero and emits the error to stderr. Invalid command arguments and connection/setup failures also precede ingestion audit initialization.

Business writes happen after staging, in one transaction: delete only the affected month's current accepted/quarantined/Gold rows, insert replacements, record the source version if new, update its pointer and commit. A source correction is a new digest, not an extra append. Every attempted ingest receives a separate audit, including replays.

## Recovery scenarios

| Symptom | Check | Recovery |
|---|---|---|
| Contract/type rejection | Last CLI error, `run_audit.error_type`, retained raw schema | Review upstream change. Fix contract with tests or use a separate versioned warehouse; rerun from retained source. Do not weaken validation only to make a run green |
| Failure after delete/insert | Audit `failed`; prior month remains committed | Rerun the same retained Parquet after fixing runtime problem. Tests inject failures after DELETE and before COMMIT |
| Report write/pointer failure | Audit `committed_export_failed`; current DB may be newer than pointer | Run `mobility-lakehouse report --data-dir data/live`. Identical ingest replay also regenerates exports |
| Earlier digest delivered late | Historical-version guard raises error | Inspect current versus historical manifests. Usually keep current. An intentional rollback uses `--allow-revert` explicitly |
| Raw checksum mismatch | Retained object no longer matches its digest | Stop. Restore from a separately verified backup to a clean directory; never change the manifest to match corruption |
| Interrupted run | Audit may remain `running`; inspect the committed pointer/source and raw evidence | Confirm no writer remains, reopen database for DuckDB recovery, then replay. Do not manually delete rows because the audit did not reach a terminal state |
| Unknown/bad records spike | `quality_summary`, source accepted/quarantine counts | Investigate source and rule applicability. All-quarantined months are permitted, so success alone is not a quality acceptance threshold |

Replay retained bytes with provenance, for example:

```bash
mobility-lakehouse ingest \
  --file data/live/raw/2024-01/REPLACE_WITH_RECORDED_SHA/source.parquet \
  --month 2024-01 --data-dir data/live \
  --source-url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

The SHA above is an explicit placeholder, not a shell variable or a real file. Read the actual digest from `sources` or the manifest. Use the same command with `--allow-revert` only for an intentional, reviewed rollback to a superseded source.

## Evidence, observability and restore

Logs contain run ID, month, row count, replay flag or error class. Detailed exception text is printed by the CLI but not stored in published reports. Useful operational signals are quarantine fraction, unexpected additive schema fields, source age, time since successful run, failed/unfinished audits and stale report pointer. No local scheduler or notification service is silently installed.

Stop writers before copying `warehouse.duckdb`, any WAL, raw directories and manifests as one backup set. Verify source digests after copying. To rebuild a lost warehouse, start a new data directory and ingest the **selected current** file for each month in the source registry; ingesting every historical version in arbitrary order is not a restoration policy. Compare counts and signed decimal totals before switching consumers. The tests demonstrate transform replay and report recovery; a full backup/restore disaster-recovery drill is not claimed.

Unreferenced report generations can be removed after confirming no writer is running and retaining the directory named by `reports/latest.json`. Raw files have no automatic retention deletion. Review recovery/audit obligations before pruning any superseded source. Local deletion of a dedicated demo data directory is optional and affects only that demo; scripts do not delete user directories automatically.

## Cloud operations

See [AZURE.md](AZURE.md) for staged bootstrap, unvalidated IAM/network assumptions, budget approval, compute stopping and resource-group teardown. Neither GitHub CI nor the local quickstart needs cloud credentials. There is no active paid deployment or Azure workload schedule.
