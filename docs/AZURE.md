# Azure target: implemented assets, deployment still pending

**No Azure subscription, resource, Databricks job, SQL warehouse or hosted dashboard was created for this project. No cloud integration, authorization, performance or disaster-recovery test has run.** The runnable local implementation uses DuckDB, not Spark or Delta Lake. These Azure files are original deployment and transformation code that has passed the specific offline checks below; they are not evidence of a production deployment.

The target preserves the selected guide's architecture: Data Factory → retained ADLS Gen2 Parquet → Databricks Bronze/Silver/Gold Delta → Databricks SQL dashboard, governed by Unity Catalog and monitored through Azure Monitor. The guide is a design reference; no restricted course code or assets were copied.

## What exists

| Asset | Implemented behavior | Validation boundary |
|---|---|---|
| `infra/main.bicep` | HNS lake, private containers, Data Factory system identity, scoped data-plane roles, Databricks Premium workspace and access connector, parameterized binary capture, Log Analytics, ADF failure alert | Compiled locally; not deployed |
| `infra/orchestration.bicep` | Second-phase ADF pipeline invokes raw capture, then a named Databricks job with the captured run's volume path | Compiled locally; identity/job integration untested |
| `infra/dev.bicepparam` | Explicit lab profile; `deployLab=false` creates no resources | Compiled locally |
| `infra/prod.bicepparam` | Documents production intent; deliberately fails the dev-only parameter contract | Expected compiler rejection observed |
| `azure/databricks/contract.py` | Pure Python versioned normalization, cent rounding, multi-reason quarantine, negative adjustment classification | Eight deterministic unit checks passed |
| `azure/databricks/transform.py` | SHA verification, strict Arrow contract, zero-based physical row identity, bounded staging, complete Bronze version append, atomic current-month Delta replacement | Python syntax checked; Spark/Delta execution not run |
| `azure/databricks/bootstrap.sql` | UC catalog/schema/volume and scoped principal grants after credential/location setup | Not executed |
| `azure/databricks/views.sql` | Silver/quarantine and Gold views over one transactional Delta table | Not executed on Databricks |
| `azure/databricks/dashboard.sql` | Six SQL datasets for quality, demand, zones, rejects, duplicates and revision history | No warehouse or dashboard created |
| `azure/databricks/render_job.py` | Credential-free renderer for a serialized, bounded job on an approved cluster | Python syntax checked |
| `azure-pipelines.yml` | Credential-free validation; separate explicit lab deployment option and environment approval boundary | YAML parsed locally; Azure DevOps has not run |

Observed on 2026-11-03: Bicep CLI **0.46.1**, two templates and dev parameters compiled without template warnings. Production parameters produced the intended **BCP033** allowed-value error. Azure CLI's newer-Bicep availability notice is not a template diagnostic. Ruby's YAML parser accepted the DevOps definition. Compilation cannot establish resource quota, runtime API support, Unity Catalog access, private connectivity, or real deployment success.

Run the cloud code's **offline** checks from the repository root:

```bash
python3 -m unittest discover -s azure -p 'test_*.py' -v
python3 -m py_compile azure/databricks/contract.py azure/databricks/transform.py azure/databricks/render_job.py
# Requires Azure CLI with Bicep installed; does not log in or deploy:
python3 azure/validate_infra.py
```

## Deployment boundary and spending

Nothing should deploy until an Azure account exists and the owner approves an explicit AUD spending plan. `deployLab=false` is the Bicep default; the Azure DevOps deployment stage is absent unless its boolean parameter is selected. No triggers, schedules, billing accounts, service connections or new cloud permissions were created.

A future estimate should price **Australia East** availability and alternatives, one Databricks driver plus one worker, up to 30 minutes per monthly load, three source months, SQL warehouse size/runtime, retained storage, ADF copy/activities and monitor ingestion/alerts. This is an input worksheet, **not a price estimate or capacity promise**. Recheck supported runtime/VM SKUs, currency/tax and trial eligibility before approval. Azure Databricks charges and underlying Azure compute are separate; a trial does not make the complete stack permanently free. Storage and log retention may continue after compute stops. Budget alerts are notifications rather than a universal spending cap.

The supplied renderer uses an **existing approved compute cluster**, with an operator-set 15-minute idle auto-termination and a 30-minute job timeout. This makes compute policy and permissions explicit during the first integration test. Existing all-purpose compute can cost more than ephemeral job compute. A later change to a policy-constrained job cluster needs a fresh estimate and live verification; do not claim that optimization is already implemented. Set SQL warehouse auto-stop before starting it and shut it down immediately after a bounded dashboard check.

## Future lab deployment sequence

The following is a runbook for **after** deployment/budget approval, not a request to execute it now.

1. Choose a dedicated resource group containing only this lab. Configure an Azure DevOps workload-identity service connection, scope it to that group, and configure independent approval/checks on the `mobility-lab-approved` environment. Role-assignment creation needs separate authorization; do not grant subscription Owner just to make a deployment work. Record the approved region, SKUs, duration, AUD estimate and approval reference.
2. Run validation first. Inspect the compiled foundation and a resource-group what-if. The lab creates authenticated public endpoints. The dev-only Bicep contract intentionally refuses production parameters.
3. Run the opt-in foundation deployment through the pipeline with `deployLab=true`, the reviewed service connection, resource group, and budget approval reference. This creates the foundation only, not compute or UC objects. Capture the deployment outputs and resource IDs as private operational evidence; do not commit credentials or account identifiers to this public repository.
4. Attach/verify the Databricks Unity Catalog metastore in the approved region. An authorized catalog administrator creates storage credential `mobility_storage` using the access connector ID. Create `mobility_raw` and `mobility_processed` external locations over the two containers. **Disable automatic file events** because this bounded batch pipeline does not grant storage-queue rights. Mark the raw location read-only. Bind the credential/locations to the intended workspace.
5. Substitute the reviewed account and principal names in `bootstrap.sql`, then execute it as the authorized catalog administrator. The job principal gets only this catalog/schema and raw volume access, plus ownership of the tables it creates. Analysts receive explicit serving-view SELECT grants later, not all-schema or raw access.
6. Import `contract.py` as a workspace Python **file** and `transform.py` as a Python **notebook** into the same workspace directory. Use a supported UC-enabled runtime with PyArrow and `TIMESTAMP_NTZ` support. Confirm that notebook-relative Python imports work on the chosen runtime. This dependency and runtime check is still pending.
7. Create or select the approved cluster, apply its access/compute policy and idle termination, then render a job definition. The command below only writes JSON; it does not create a job:

   ```bash
   python3 azure/databricks/render_job.py \
     --cluster-id "$MOBILITY_APPROVED_CLUSTER_ID" \
     --notebook-path /Shared/mobility/transform > /tmp/mobility-job.json
   ```

   Review the JSON, create the job through the authenticated Databricks CLI/UI, and record its ID. The job has concurrency one, queueing, a 30-minute timeout and one retry. Do not create multiple independent copies writing the same tables. Grant the ADF managed identity the required workspace entitlement and job `CAN_MANAGE_RUN` permission; keep the actual job's run-as principal restricted. Test managed-identity authentication explicitly. The ADF linked service also uses the workspace resource ID; any Azure resource-level permission required by that authentication path must be reviewed separately. This repository does not silently grant ADF Contributor over the Databricks workspace.
8. Deploy `infra/orchestration.bicep` with the actual factory name, workspace name and job ID. It references the foundation resources and creates `CaptureAndPublishMonth`. It does not bootstrap workspace identities, create the job or start compute.
9. Manually run January 2024. The child ADF capture copies original bytes to `raw/yellow/2024-01/<child-run-id>/source.parquet`; the parent passes that exact path to the Databricks job. The job hashes the retained bytes before parsing. Run-id paths prevent later runs from replacing this source. Copy retries can rewrite their own in-progress destination; that destination is not a committed version until capture succeeds and hashing validates it. This is application immutability, **not WORM storage or a defense against a privileged storage administrator**.
10. After the first successful load creates the Delta tables, install `views.sql`. Grant analysts SELECT on `gold_hourly_zone` and `gold_quality` only; grant quarantine/duplicate investigation access separately to operators as needed. Check UC lineage and that analysts cannot read raw/Bronze data or bypass UC through direct storage permissions.
11. Run February and March, then execute the acceptance tests below. Create a small SQL warehouse only within its approved budget window. Save the queries in `dashboard.sql` as dashboard datasets and configure the displayed fields/visualizations. There is no pre-existing hosted dashboard to claim or link.
12. Collect evidence, terminate compute, stop the SQL warehouse and follow cleanup. Save the measured costs and all unexecuted checks honestly.

The main pipeline's source-month allowlist is January–March 2024. A corrected delivery requires `allowRevision=true` after comparing its SHA and source provenance. Normal repeated bytes are a no-op in the business state.

## Publication and recovery semantics

The notebook uses bounded **driver-side PyArrow batches of 16,384 rows** to preserve exact file row offsets without treating content hashes as unique trips. It then stages Spark DataFrames in a disposable managed Delta table. This is a deliberate reference implementation for three monthly files, not a demonstrated distributed extraction benchmark. Limits are 256 MiB compressed, 10 million rows, 128 columns and 5,000 row groups per file; those do not constitute a hard decompressed-memory bound. A pathological nested extra column can still consume substantial memory and must be tested before broadening the source contract.

A Bronze row contains source month, SHA256, zero-based physical ordinal, retained URI, byte count, normalization UTC time, Arrow schema JSON, contract version, raw payload and normalized values/reasons. ADF does not calculate a SHA manifest. The notebook calculates the SHA after capture; by default orchestration leaves the optional expected SHA/byte arguments empty. Therefore there is no independent upstream checksum comparison and no separate ADLS JSON manifest yet; cloud provenance lives in the retained run path, ADF run logs and Bronze fields. The retained Parquet is the exact-byte source of truth; the JSON representation is inspectable parsed data and converts non-finite values to strings. `captured_at_utc` in the cloud code is **normalization time**, not the original TLC publication time or historical freshness.

After staging and conservation checks, one Delta append commits the entire Bronze version. Existing version rows are reused on retry. Every current row is regular, adjustment or quarantined, and regular + adjustment + quarantined must equal the Parquet metadata count. Exact source duplicates remain separate rows. Candidate duplicate matching includes normalized pickup/drop-off, locations, distance, amounts, optional vendor/passenger/payment fields and raw-sign adjustment flag. Groups of N flag N−1 candidates; no deletion follows automatically.

The current table uses one `replaceWhere source_month=...` commit. Silver, quarantine and Gold are **views over that same current table**, so no claim of a transaction across multiple Delta tables is necessary. A failed current overwrite leaves the old month available. A later successful overwrite changes only that source month. Current-month idempotence includes the contract version; future contract changes require explicit versioning and revalidation.

| Interruption | Expected recovery design; live exercise pending |
|---|---|
| ADF download fails | Retain the failed run's evidence, rerun capture to a new run path; do not treat incomplete bytes as a source version |
| Schema/type contract fails | Raw remains; no Bronze/current business publication; inspect additional/missing fields and change the versioned contract deliberately |
| Batch normalization fails | Current unchanged; notebook `finally` drops its staging table when possible; hard-killed jobs can leave `_stage_*` tables requiring age/run-aware operator cleanup |
| Bronze append commits, current replacement fails | Retry the same retained file; reuse the complete Bronze version, recheck count, and retry the atomic current replacement |
| Current replacement succeeds but response is lost | Retry recognizes same current SHA + contract version and returns replayed; no duplicate trip facts |
| Incorrect source was deliberately published | Inspect retained prior source, then replay it with revision authorization; compare affected month and untouched months. This is an explicit revert, not automatic last-arrival selection |
| Duplicate writers or unexpected Bronze count | Stop writers; investigate. Do not delete rows blindly to force a count match. Only one job identity/job may publish at a time |
| Region/resource-group loss | Recovery depends on retained exports/backups and tested replication; **no regional disaster recovery is implemented or proven** |

NYC timestamps are naive local civil values, stored as `TIMESTAMP_NTZ`. March crosses daylight-saving time. Subtracting these values does not establish physical elapsed travel duration, and hourly buckets are local clock labels. Do not silently label them UTC. Negative source money is classified as an adjustment even if it rounds to zero cents; adjustments are displayed separately and included in signed totals. Totals represent recorded amounts, not net profit or unique passengers.

## Cloud acceptance evidence still required

- Repeat the committed January file and compare deterministic business-state counts/totals and Delta history; unchanged inputs must not create duplicate facts.
- Submit a corrected January version, verify current January changes exactly once, verify February/March are unchanged, and show retained prior provenance/history.
- Exercise missing/nonnumeric/tz-aware schema fields, an additive field, bad timestamps, invalid locations, non-finite values and distance-total overflow. Reconcile accepted plus quarantined rows; count multiple reasons without double-counting the rejected row.
- Interrupt staging, Bronze completion and current publication separately; prove restart behavior against retained raw bytes. Test corrected/revert order and serialization.
- Compare the cloud contract's synthetic fixture and live-file aggregates with local evidence. Offline contract tests are not proof that Spark decimal, NTZ, Delta or UC behavior matches the local engine.
- Verify denial tests for analyst raw/Bronze access and cross-workspace access, managed-identity scope, table ownership, credential binding and lineage. Public-endpoint lab access is not production network isolation.
- Measure actual runtime, peak driver/executor resources, read/write bytes, reject/duplicate/adjustment rates, source retention growth, dashboard query latency and cost. No performance SLA is asserted in this repository.
- Force an ADF failure and verify alert routing through an explicitly configured action group. The default alert has **no notification receiver**. Log Analytics has a 1 GB/day ingestion cap and 30-day retention; caps can delay/drop telemetry and do not cap all Azure spending. Data quality/freshness/backlog alerts beyond the ADF failure metric remain future work.

## Production gaps and teardown

Production deployment is blocked in code. Before changing that guard, implement and validate VNet injection, approved egress, private endpoints/DNS for storage/workspace/data-plane paths, ADF private connectivity, workspace/private auth paths, identity lifecycle and DR/retention policies. No parameter called “production” should quietly enable the current public-endpoint lab. Key Vault is not deployed because this design uses managed identities and has no unavoidable application secret; introduce it only if a documented integration requires a secret.

After a lab: stop/terminate compute and SQL warehouses, disable any manually added triggers, inspect the dedicated group's resources and the Databricks managed resource group, export the agreed evidence/raw snapshots, then delete only the reviewed lab resources using the Azure Portal or reviewed commands. Resource-group deletion is destructive: confirm that it contains no unrelated resources and that retention requirements are met. Verify that managed disks, public IPs, monitor resources and retained storage were removed or intentionally retained. A stopped cluster does not remove retained-storage costs. No deletion or deployment is performed by the offline validation commands.

## Primary technical references

Accessed 2026-11-03. API/behavior choices should be rechecked before live deployment.

- [ADF binary format](https://learn.microsoft.com/en-us/azure/data-factory/format-binary) and [ADLS connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage?tabs=data-factory): binary preservation and managed-identity lake access.
- [ADF Databricks Job activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-job) and [compute linked services](https://learn.microsoft.com/en-us/azure/data-factory/compute-linked-services): job parameters and authentication integration.
- [Databricks workspace ARM reference](https://learn.microsoft.com/en-us/azure/templates/microsoft.databricks/2024-05-01/workspaces) and [access connectors](https://learn.microsoft.com/en-us/azure/templates/microsoft.databricks/2024-05-01/accessconnectors): infrastructure API shapes.
- [Unity Catalog ADLS credentials](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/storage-credentials), [managed identities](https://learn.microsoft.com/en-us/azure/databricks/getting-started/connect-to-azure-storage), and [external locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/manage-external-locations): identity, least privilege and file-event settings.
- [Delta selective overwrite](https://learn.microsoft.com/en-us/azure/databricks/delta/selective-overwrite): one-table replacement semantics.
- [ADF monitoring](https://learn.microsoft.com/en-us/azure/data-factory/monitor-data-factory) and [supported metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-datafactory-factories-metrics): failure detection.
- [Azure Databricks pricing](https://azure.microsoft.com/en-us/pricing/details/databricks/), [ADF pricing](https://azure.microsoft.com/en-us/pricing/details/data-factory/data-pipeline/), and [Azure Monitor costs](https://learn.microsoft.com/en-us/azure/azure-monitor/cost-usage): inputs for a future approved AUD estimate, not an assertion that this lab is free to deploy.
