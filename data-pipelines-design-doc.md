# Aviation Data Pipeline

Two independent implementations transform OpenSky data on Azure Databricks:

1. **Bronze Avro → Silver Delta:** Spark Structured Streaming discovers new files, rereads each complete affected Bronze hour and atomically overwrites the corresponding hourly Silver partitions.
2. **Silver Delta → Gold Delta:** A scheduled Databricks SQL file task runs on a SQL warehouse and atomically refreshes country/time aggregates.

```text
AKS collector → hourly ADLS Bronze Avro
                        ↓ Spark Structured Streaming
                   Silver Delta
                        ↓ scheduled Databricks SQL
                    Gold Delta → analyst SQL
```

**Status:** Bronze → Silver code, a paused continuous Job definition and local Spark/Delta tests are available. Provisioned DBR/UC/ADLS integration and replay are not yet verified; Gold SQL/jobs remain design only. In a parent platform checkout, see the [architecture and technical design](../references/thales-azure-data-platform-design.md).

## Planned Tech Stack

PySpark / Spark Structured Streaming, native Avro file reader, Delta Lake, Unity Catalog, Databricks Jobs, and Databricks SQL on a Pro or Serverless SQL warehouse. No Kafka, Auto Loader, file-arrival Job trigger or processed-file manifest is required.

## Input: Immutable, Source-Shaped Bronze

Ingestion is owned by the separate collector. It archives one Snappy-compressed Avro object container file with one record per API response:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/
  opensky/states/year=YYYY/month=MM/day=DD/hour=HH/<response.time>.avro
```

- Bronze has **exactly `time` and `states`**. `states` is null or an array of original 17-element positional arrays, not named aircraft records. Do not expect enrichment fields in Bronze.
- Decoding preserves source structure, types, nulls, padded strings and the nested sensors array. It does not reproduce exact JSON bytes.
- Derive hourly paths from `time` in UTC. The fixture timestamp `1793013825` gives `year=2026/month=10/day=26/hour=11/1793013825.avro`.
- Canonical files never change. Duplicate/conflicting snapshots and quarantine publication are handled by ingestion; quarantine stays outside the watched source root.
- Hourly partitions group files; they are not overwritten or combined into one hourly object.

## Implementation 1: Bronze → Silver Streaming

### Reader and execution

- Use `spark.readStream.format("avro")` with an **explicit Spark schema** matching the producer schema and selected runtime's Avro-union representation.
- Read a stable Bronze root with `recursiveFileLookup=true` and `pathGlobFilter=*.avro`. This discovers nested hourly directories without relying on inferred partition columns; derive timestamps from payload `time`.
- Maintain one long-running query on **classic Jobs compute** under a continuous Databricks Job trigger. Start with a **one-minute processing-time micro-batch trigger**, then tune using measurements. Serverless Jobs compute does not support this trigger; a Serverless SQL warehouse for Gold is separate. This is not Spark continuous-processing mode: `foreachBatch` needs micro-batches.
- Use a unique durable checkpoint, separate from Bronze and managed-table storage, for example `/Volumes/aviation/ops/streaming_checkpoints/opensky/bronze_to_silver/`. Preserve it across ordinary restarts; do not apply automatic expiry.

### Flattening and validation

Apply these steps to the **complete-hour batch reread**, not only the newly discovered files.

1. Decode union branch values before mapping each of the 17 source positions into named, typed Silver columns. Complex Avro unions can appear as Spark structs; do not assume `states` already contains named records.
2. Derive `snapshot_time`, UTC snapshot timestamp, `snapshot_hour_utc` (`floor(snapshot_time / 3600) * 3600` epoch seconds) and five-minute UTC bucket (`floor(snapshot_time / 300) * 300`) from top-level `time`. Configure Spark and SQL sessions for UTC. Keep all source observation fields, trim callsigns only in Silver, and name measurement units explicitly.
3. Keep optional null positions/altitudes. Reject/count invalid aircraft identifiers or out-of-bounds coordinates with Bronze file/row provenance in restricted logs; preserve the original in Bronze and fail if required accounting cannot be recorded. The stream does not move/rewrite source records. Schema/type/length failures stop processing visibly rather than silently skipping data.
4. Before flattening loses reader metadata, capture the Bronze file path for provenance; verify file-metadata support on the pinned runtime.
5. Handle empty batch DataFrames and treat null/empty `states` as successful zero-observation snapshots. Record source-processing freshness in structured logs or optional Ops audit, not by fabricating Silver rows or adding Bronze fields.

### Deduplication and write contract

- Target managed table: `<catalog>.silver.aircraft_states`, **partitioned by `snapshot_hour_utc`**, including the full UTC date.
- Business key: **`(snapshot_time, icao24)`**. The same aircraft in a later snapshot is a new observation.
- In `foreachBatch`, identify affected UTC hours from input `time` **before exploding `states`**. Reread **all canonical Avro files** in each corresponding Bronze hour directory using a batch reader with the same schema. Validate timestamps against the requested directories; read errors must fail, not become empty results.
- Normalize the complete hours and deterministically select one row per key: greatest `last_contact`, then `time_position`, then stable observation-content hash. The hash is transformation-only, not a Bronze field. Label rejection counts by rebuild scope rather than treating rereads as new observations.
- Write all affected hours in **one atomic Delta overwrite with an explicit `replaceWhere` predicate on `snapshot_hour_utc`**. Keep constraint checking enabled and preserve the table/schema/grants. No Silver MERGE is needed. Never overwrite all Silver, or replace an hour with only the incoming micro-batch.
- No affected hours means no write. An affected hour with zero valid observations still needs a schema-correct **empty replacement**, clearing that hour only; do not skip it because normalized output is empty. Dynamic partition overwrite cannot clear an absent output partition.
- Retain immutable canonical Bronze for at least 30 days. Refuse expired, unreadable or path-inconsistent discovered files and missing/unreadable source in the selected hour; only present files can be validated. A previously deleted file cannot be detected without a separate inventory: prevent deletion through storage permissions and backups, confirm retention before replay, and do not use prior Silver rows to mask lost Bronze.
- Serialize stream/backfill writers and propagate failures. Checkpoint progress follows successful processing; replay rereads and replaces full hours without duplicate business keys. Business rows remain stable for unchanged Bronze/rules, while `processed_at_utc` may change.

**Hourly is the replacement scope, not the refresh cadence:** keep the streaming trigger, refreshing the current hour as files arrive. Late files rebuild their original hour; a micro-batch can affect several hours. This is simpler than key-based MERGE but rereads/rewrites earlier observations repeatedly. Benchmark that cost before scaling.

## Implementation 2: Silver → Gold Scheduled SQL

- Target managed table: `aviation.gold.aircraft_activity_5m`.
- Execute a version-controlled **SQL file task on a Pro or Serverless SQL warehouse**, independently of the Silver stream. No separate PySpark Gold job is required.
- Initially schedule every **two minutes in UTC**, with one active refresh. Gold's schedule and permissions are separate from the stream.
- At PoC scale, recompute **all retained Silver** and publish with one atomic **`INSERT OVERWRITE TABLE`** statement. Preserve the existing Gold catalog object/grants. Do not use separately committed delete/insert statements.
- Full refresh handles retries and late historical data and removes obsolete country groups. Upsert-only aggregate MERGE can retain stale groups; additive counters inflate results on replay.
- Benchmark full-scan cost at larger scale; introduce atomic affected-bucket replacement only when necessary. No file-completion ledger is needed for this SQL refresh.

### Gold definitions

| Value | Meaning |
|---|---|
| `bucket_start_utc` | Five-minute interval derived from source snapshot time |
| `origin_country` | Latest selected observation's registration-country proxy; missing/blank becomes `Unknown` |
| `aircraft_count` | One latest observation per `(bucket, icao24)`, then count by country |
| `avg_baro_altitude_m` | Mean of each counted aircraft's latest non-null barometric altitude **within that bucket**; never carry across buckets |
| `altitude_sample_count` | Aircraft contributing a non-null altitude |
| `last_source_time` | Latest selected source timestamp represented in the group |
| `updated_at_utc` | SQL refresh time |

`origin_country` is **not** the aircraft's geographic location or operator country. Retain at least 24 hours of history after warm-up. In a parent platform checkout, the [Gold design and SQL example](../references/thales-azure-data-platform-design.md#8-gold-scheduled-databricks-sql-country-activity) defines the atomic refresh, including the latest-null/earlier-valid altitude case.

## Recovery and Freshness

- Restart the Silver stream with its existing checkpoint. A commit-before-checkpoint failure rereads and replaces the complete affected hours.
- Pause the normal stream for explicit bounded historical backfills, wait for the active run to stop, then run **the same Job** with `--replay-from`/`--replay-to` UTC epoch seconds (inclusive, rounded to complete hours). Confirm Bronze retention/backup, run bounded replay, compare Silver keys/counts against Bronze, then refresh Gold and resume. Never rebuild from only selected snapshots. File-age filters and old modification times must be checked; path timestamps alone do not guarantee old restored files will be discovered.
- Missing/corrupt/incompatible checkpoints require controlled replay and validation, not an automatic reset. Never overwrite canonical Bronze to force reprocessing.
- Malformed canonical files stop the PoC stream and alert until resolved through a controlled procedure. Do not enable silent corrupt-file skipping.
- Gold failures leave prior Gold available while Silver continues. Retry the SQL task independently; the stream checkpoint does not certify Gold freshness.
- Monitor source timestamp age (including empty polls), stream progress/backlog, Silver/Gold commits, SQL task success and compute uptime. Business aggregates are stable on replay; refresh audit timestamps may change.
- A target of p95 <5-minute source-to-Gold freshness needs measurement. Polling, stream lag, SQL scheduling and warehouse startup all contribute; trigger intervals alone prove nothing.

## Identity and Deployment Inputs

Use separate non-human Run-as principals. The stream reads Bronze, writes its checkpoint and reads/modifies Silver. The SQL task reads Silver, reads/modifies Gold and uses its SQL warehouse. Neither needs OpenSky credentials or storage-account keys. Analysts receive read-only Silver/Gold access, not Bronze, checkpoints, secrets or write permissions.

Terraform/platform bootstrap supplies the workspace/UC objects, Access Connector storage access, volumes, managed tables, principals/grants, Spark cluster policy and SQL warehouse. This submodule defines the Silver Job; Gold is not implemented. Runtime inputs include source root, protected checkpoint path, pinned schema/runtime, and table name. Do not put secrets in source control or Terraform state.

## Implementation and Verification Checklist

Silver paths: [`bronze_silver/stream.py`](bronze_silver/stream.py), [`databricks.yml`](databricks.yml), [`tests/test_stream.py`](tests/test_stream.py) and the producer-written [`tests/fixtures/1793013825.avro`](tests/fixtures/1793013825.avro). Gold SQL and Job paths remain planned.

- Implement/pin producer-compatible schemas and Spark Avro decoding; verify exact fixture values, all 17 positions, mixed numeric/boolean types, nulls and nested sensors.
- Test hourly partition derivation, new directories, UTC hour/day boundaries, late files and valid zero-row snapshots.
- Test a second snapshot preserving the earlier contents of its hour, multi-hour replacement, unrelated-hour preservation, explicit empty replacement and refusal of incomplete-source reads.
- Test duplicate keys across full hours, deterministic survivors, invalid records, checkpoint restart and failure after Silver commit but before checkpoint advancement.
- Test SQL retry, latest-country changes removing old groups, latest-null altitude fallback within the same bucket, all-null sample counts and late historical corrections.
- Verify analyst access denies Bronze/checkpoints/secrets/write actions; run a bounded replay with no concurrent Silver writer.
- Measure live end-to-end freshness, stream/warehouse uptime and actual cost. Continuously running Spark and frequent SQL refreshes have idle costs.
- Provisioned verification is outstanding: validate/deploy the bundle, confirm rights and restart/replay on DBR 15.4 before resuming the trigger. Teardown must preserve the Bronze, Silver and checkpoint backup as required by retention policy.

## Running Silver (local and provisioned)

From this directory, install the pinned **test-only** Spark 3.5.0 / Delta 3.2.0 packages with Python 3.11 and JDK 17 (the local tests resolve Spark Avro/Delta jars from Maven):

```sh
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-test.txt
export JAVA_HOME=/path/to/jdk-17
PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v
```

The Avro fixture is written by the pinned producer, not hand-authored JSON. Tests cover all positions, union branches, nulls/sensors, UTC hour derivation, late and duplicate files, rejections, empty-hour clearing, missing files and checkpoint restart. These **do not** prove Databricks identity, UC volume metadata, Delta privileges or the hosted runtime. Provision the [two-account platform and UC permissions](../infra/tf/README.md) first; the primary Bronze account has no blob soft delete, while the separate checkpoint account has 30-day blob soft delete and no automatic checkpoint expiry. Give the stream run-as read on Bronze, read/write on checkpoints and SELECT/MODIFY on Silver. Confirm Azure credentials and DBR `15.4.x-scala2.12` match the Terraform policy and Spark 3.5 union/metadata behavior.

Deploy with the Databricks CLI from this directory (Azure CLI logged in as an operator authorized to deploy/run as the Silver principal). These Terraform outputs must be from the **same** environment; use the actual `spark_node_type_id` supplied to Terraform. Do not deploy over an active writer:

```sh
export DATABRICKS_HOST="$(terraform -chdir=../infra/tf/azure output -raw workspace_host)"
export DATABRICKS_AUTH_TYPE=azure-cli
ENV=sit
VARS=(
  --var "environment=$ENV"
  --var "catalog=$(terraform -chdir=../infra/tf/workspace output -raw catalog_name)"
  --var "source_root=$(terraform -chdir=../infra/tf/workspace output -raw bronze_volume_root)"
  --var "checkpoint_root=$(terraform -chdir=../infra/tf/workspace output -raw checkpoint_volume_root)"
  --var "silver_run_as=$(terraform -chdir=../infra/tf/workspace output -raw silver_run_as)"
  --var "silver_policy_id=$(terraform -chdir=../infra/tf/workspace output -raw silver_policy_id)"
  --var "spark_node_type_id=<matching Terraform node type>"
)
databricks bundle validate "${VARS[@]}"
databricks bundle deploy --fail-on-active-runs "${VARS[@]}"
databricks bundle summary "${VARS[@]}"
```

The versioned Job is **PAUSED by default**. Inspect the uploaded Python task, cluster policy, run-as, checkpoint and permissions; copy `JOB_ID` from the bundle summary/job URL. Verify producer Avro in Bronze, including the empty/null `states` cases, and the protected checkpoint path. Then start exactly one writer:

```sh
JOB_ID=<bundle Job ID>
databricks jobs update "$JOB_ID" --json '{"new_settings":{"continuous":{"pause_status":"UNPAUSED"}}}'
databricks jobs list-runs --job-id "$JOB_ID" --active-only
```

Observe the driver `silver_commit` and restricted `silver_reject` JSON logs, streaming progress/lag, Silver keys/hours/partition count, and checkpoint files after the first run. A source-aged empty poll will not create a Silver row. Restart the same Job with the **same** checkpoint and confirm no duplicate `(snapshot_time, icao24)`; append a late canonical file and confirm the old hour is rebuilt without deleting an unrelated hour. The stream fails closed for path mismatch, malformed Avro/states, unreadable files or failed audit/write; alert and restore from backup instead of enabling corrupt-file skipping. A log/audit write failure must prevent checkpoint acknowledgement. Provisioned first-run and recovery checks are still outstanding.

For a controlled historical replay (UTC epoch-second start/end, **inclusive**; only the last 30 retained days), first pause the trigger, **wait for or cancel the active run**, and verify `list-runs --active-only` is empty. Audit/restore complete immutable Bronze from backup, including previously deleted files that cannot be inferred from present files; do not merge old Silver to fill gaps. Use **Run Now on this same one-writer Job**, overriding all Python parameters (including the required checkpoint argument, unused by replay):

```sh
databricks jobs update "$JOB_ID" --json '{"new_settings":{"continuous":{"pause_status":"PAUSED"}}}'
databricks jobs list-runs --job-id "$JOB_ID" --active-only
# Replace the paths, table and numeric UTC bounds; this runs complete hours covering the interval.
databricks jobs run-now "$JOB_ID" --json '{"python_params":["--source-root","/Volumes/<catalog>/bronze/opensky_states","--checkpoint","/Volumes/<catalog>/ops/streaming_checkpoints/opensky/bronze_to_silver","--table","<catalog>.silver.aircraft_states","--replay-from","<start_epoch>","--replay-to","<end_epoch>"]}'
```

Wait for the replay run to succeed before another hour or resume; compare business keys/counts for every affected Silver UTC hour to the retained Bronze files and check unrelated hours stayed unchanged. `processed_at_utc` may differ on replay. When the independent Gold SQL Job is implemented, run `databricks jobs run-now <gold-job-id>` and verify Gold freshness **before** declaring end-to-end recovery; it is not deployable from this submodule yet. Resume the trigger with the UNPAUSED command above. If a checkpoint is missing/corrupt/incompatible, **stop**, back it up and explicitly approve a new checkpoint/replay plan: replay all retained hours that must be repaired, then reconcile/restore the stream's discovery state; a new checkpoint can rediscover data and cannot recover files older than file-source discovery bounds by itself. Never silently delete/reset the original checkpoint. For teardown, pause and stop the Job, back up its checkpoint, then review and run `databricks bundle destroy "${VARS[@]}"` to remove only the bundle-owned Job/code; preserve Bronze, the protected checkpoint backup and the Terraform-owned managed Silver table according to the recovery plan. Continuous Spark has idle cost even with no aircraft.

## Source Data Spec

1. [OpenSky REST API](https://openskynetwork.github.io/opensky-api/rest.html)
2. [Original response fixture](example_response.json). Bronze stores the same two-field structure and positional arrays:
```json
{
  "time": 1793013825,
  "states": [
    [
      "8a044f",
      "GIA190  ",
      "Indonesia",
      1793013823,
      1793013825,
      99.9767,
      1.7853,
      10927.08,
      false,
      227.31,
      322.17,
      -5.85,
      null,
      11673.84,
      null,
      false,
      0
    ]
  ]
}
```
