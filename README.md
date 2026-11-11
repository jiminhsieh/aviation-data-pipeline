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

**Status:** Silver streaming and Gold SQL, separate paused Jobs and local Spark/Delta tests are available. Azure/Databricks deployment, hosted runtime, grants, replay and analyst access remain unverified. In a parent platform checkout, see the [architecture and technical design](../references/thales-azure-data-platform-design.md).

## Tech Stack

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

- Target Terraform-managed table: `<catalog>.gold.aircraft_activity_5m` (`aviation` in `sit`). The runnable derivation is [`sql/silver_to_gold.sql`](sql/silver_to_gold.sql), deployed by the independent `silver_to_gold` Job in [`databricks.yml`](databricks.yml).
- Execute a version-controlled **SQL file task on the Terraform Pro SQL warehouse** (or a compatible Serverless warehouse), independently of the Silver stream. The task takes `catalog`, `gold_run_as` and `gold_warehouse_id` from that environment's Terraform outputs; no PySpark Gold job or stream callback is required.
- Schedule every **two minutes in UTC**, with `max_concurrent_runs: 1`. Both Job schedules are initially paused until provisioned verification; unpause Gold separately after its first-run checks.
- At PoC scale, recompute **all retained Silver** in a UTC SQL session and publish with one atomic **`INSERT OVERWRITE TABLE`** statement. Empty Silver clears Gold, including obsolete entire hourly partitions; failed SQL leaves the prior Delta version available. Preserve the existing Gold catalog object/schema/grants. Do not use dynamic partition overwrite, separately committed delete/insert statements or concurrent manual writers.
- Full refresh handles retries and late historical data and removes obsolete country groups. Upsert-only aggregate MERGE can retain stale groups; additive counters inflate results on replay.
- Benchmark full-scan cost at larger scale; introduce atomic affected-bucket replacement only when necessary. No file-completion ledger is needed for this SQL refresh.

### Gold definitions

| Value | Meaning |
|---|---|
| `bucket_hour_utc` | Physical full-date UTC hourly `TIMESTAMP` partition, truncated from the five-minute bucket (not hourly aggregation) |
| `bucket_start_utc` | Logical five-minute UTC interval derived from source snapshot epoch seconds (`TIMESTAMP`) |
| `origin_country` | Latest selected observation's registration-country proxy; missing/blank becomes `Unknown` |
| `aircraft_count` | One latest observation per `(bucket, icao24)`, then count by country |
| `avg_baro_altitude_m` | Mean of each counted aircraft's latest non-null barometric altitude **within that bucket**; never carry across buckets |
| `altitude_sample_count` | Aircraft contributing a non-null altitude |
| `last_source_time` | Latest selected source snapshot epoch **seconds** in the group (`BIGINT`) |
| `updated_at_utc` | SQL refresh time (`TIMESTAMP`); not source time or queryability time |

`origin_country` is **not** the aircraft's geographic location or operator country. Silver's source country is unchanged; only Gold trims and converts blank/null to `Unknown`. Count one aircraft per bucket even when altitude is null; average only the latest non-null altitude per aircraft *within that bucket*, and report null average when no aircraft has a sample. Retain at least 24 hours of history after warm-up (about 30 days intended); an empty source cannot manufacture Gold rows. The SQL file is the executable contract, tested against local Delta fixtures.

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

Terraform/platform bootstrap supplies the workspace/UC objects, Access Connector storage access, volumes, managed tables, principals/grants, Spark cluster policy and SQL warehouse. This submodule defines separate Silver and Gold Jobs. Runtime inputs include source root, protected checkpoint path, pinned schema/runtime, Silver catalog, Gold catalog, SQL warehouse ID and both run-as application IDs. Do not put secrets in source control or Terraform state.

## Implementation and Verification Checklist

Silver paths: [`bronze_silver/stream.py`](bronze_silver/stream.py), [`databricks.yml`](databricks.yml), [`tests/test_stream.py`](tests/test_stream.py) and the producer-written [`tests/fixtures/1793013825.avro`](tests/fixtures/1793013825.avro). Gold paths: [`sql/silver_to_gold.sql`](sql/silver_to_gold.sql), the separate Job in [`databricks.yml`](databricks.yml), and [`tests/test_gold.py`](tests/test_gold.py).

- Implement/pin producer-compatible schemas and Spark Avro decoding; verify exact fixture values, all 17 positions, mixed numeric/boolean types, nulls and nested sensors.
- Test hourly partition derivation, new directories, UTC hour/day boundaries, late files and valid zero-row snapshots.
- Test a second snapshot preserving the earlier contents of its hour, multi-hour replacement, unrelated-hour preservation, explicit empty replacement and refusal of incomplete-source reads.
- Test duplicate keys across full hours, deterministic survivors, invalid records, checkpoint restart and failure after Silver commit but before checkpoint advancement.
- Local Delta SQL fixtures cover retry, latest-country changes removing old groups, latest-null altitude fallback within the same bucket, all-null sample counts, UTC date boundaries, late historical corrections, empty Silver, and a failed query retaining prior Gold. Verify the same behavior on the provisioned SQL warehouse before unpausing.
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

Deploy with the Databricks CLI from this directory (Azure CLI logged in as an operator authorized to deploy/run as **both** Silver and Gold principals). These Terraform outputs must be from the **same** environment; use the actual `spark_node_type_id` supplied to Terraform. Do not deploy over an active writer:

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
  --var "gold_run_as=$(terraform -chdir=../infra/tf/workspace output -raw gold_run_as)"
  --var "gold_warehouse_id=$(terraform -chdir=../infra/tf/workspace output -raw gold_warehouse_id)"
  --var "silver_policy_id=$(terraform -chdir=../infra/tf/workspace output -raw silver_policy_id)"
  --var "spark_node_type_id=<matching Terraform node type>"
)
databricks bundle validate "${VARS[@]}"
databricks bundle deploy --fail-on-active-runs "${VARS[@]}"
databricks bundle summary "${VARS[@]}"
```

Both versioned Jobs are **PAUSED by default**. Inspect the uploaded Python task, cluster policy, run-as, checkpoint and permissions; copy `JOB_ID` from the bundle summary/job URL. Verify producer Avro in Bronze, including the empty/null `states` cases, and the protected checkpoint path. Then start exactly one writer:

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

Wait for the replay run to succeed before another hour or resume; compare business keys/counts for every affected Silver UTC hour to the retained Bronze files and check unrelated hours stayed unchanged. `processed_at_utc` may differ on replay. Run `databricks jobs run-now <gold-job-id>` and verify Gold freshness **before** declaring end-to-end recovery; the SQL task is independent of the Silver checkpoint. Resume the trigger with the UNPAUSED command above. If a checkpoint is missing/corrupt/incompatible, **stop**, back it up and explicitly approve a new checkpoint/replay plan: replay all retained hours that must be repaired, then reconcile/restore the stream's discovery state; a new checkpoint can rediscover data and cannot recover files older than file-source discovery bounds by itself. Never silently delete/reset the original checkpoint. For teardown, pause and stop the Job, back up its checkpoint, then review and run `databricks bundle destroy "${VARS[@]}"` to remove only the bundle-owned Job/code; preserve Bronze, the protected checkpoint backup and the Terraform-owned managed Silver table according to the recovery plan. Continuous Spark has idle cost even with no aircraft.

## Running Gold and checking analyst access (provisioned)

The same bundle deployment above uploads `sql/silver_to_gold.sql` as a **workspace SQL file**; `catalog` is a named SQL task parameter, not a secret. Get the **Gold** Job ID from `databricks bundle summary "${VARS[@]}"` (not the continuous Silver `JOB_ID`). Before any SQL refresh, verify Terraform created a Pro warehouse, the full-date `bucket_hour_utc TIMESTAMP` Gold partition and the expected eight columns in dictionary order. Verify the Gold run-as can use that warehouse, SELECT Silver and SELECT/MODIFY Gold, but cannot read Bronze/checkpoints or use OpenSky secrets. Keep Silver and Gold writers independent; do not create a second Gold Job/schedule for the same table.

```sh
GOLD_JOB_ID=<Gold Job ID from bundle summary>
databricks jobs run-now "$GOLD_JOB_ID"   # wait for SUCCESS; check SQL file path/parameters and warehouse
# After verifying SQL results and permissions, start the independent UTC two-minute schedule:
databricks jobs update "$GOLD_JOB_ID" --json '{"new_settings":{"schedule":{"quartz_cron_expression":"0 0/2 * * * ?","timezone_id":"UTC","pause_status":"UNPAUSED"}}}'
databricks jobs list-runs --job-id "$GOLD_JOB_ID" --active-only
```

As an **ATM analyst** with only `USE CATALOG`, `USE SCHEMA`, Gold SELECT and warehouse CAN_USE, run this in a UTC SQL editor session on that warehouse (replace `<catalog>` with the deployed catalog); confirm Silver access is optional and Bronze/checkpoint/secret/write access is denied. A scientist may additionally query Silver. Keep **both** predicates: hourly pruning can include 25 partitions, while the five-minute cutoff enforces exactly the rolling 24-hour window. No country filter converts registration-country into overflight country.

```sql
SET TIME ZONE 'UTC';

SELECT bucket_start_utc, origin_country, aircraft_count,
       avg_baro_altitude_m, altitude_sample_count, last_source_time, updated_at_utc
FROM <catalog>.gold.aircraft_activity_5m
WHERE bucket_hour_utc >= DATE_TRUNC('HOUR', CURRENT_TIMESTAMP() - INTERVAL 24 HOURS)
  AND bucket_start_utc >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
  AND bucket_start_utc <= CURRENT_TIMESTAMP()
ORDER BY bucket_start_utc DESC, origin_country;
```

After **at least 24 hours of source/Gold warm-up**, compare this query to Silver per `(bucket_start_utc, icao24)` and confirm an old partition disappears after an authorized correction and an empty Silver source clears Gold. For a short-lived deployment, use a **clearly labeled historical fixture** covering 24 hours instead; do not call a few minutes of live data 24-hour coverage. Check `MIN(bucket_start_utc)`, `MAX(bucket_start_utc)` and distinct `bucket_hour_utc` against available retained source, and verify the Gold schema/partition against the dictionary above. Run the local Gold fixtures with `PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python .venv/bin/python -m unittest discover -s tests -p test_gold.py -v`; they exercise the SQL text but not UC/warehouse parameter binding or analyst identity.

Monitor separately: source response `time` **even when `states` is null/empty**, successful Bronze publication, Silver checkpoint/progress and source hours, successful Gold SQL runs and Gold Delta commit/analyst queryability. Distinguish a healthy empty source (fresh source time, no new aircraft rows) from a stale collector, stuck Silver, or failed Gold SQL (old Gold remains queryable but stale). On SQL failure, inspect the independent Job, fix and rerun it without resetting Silver's checkpoint; the single overwrite either commits or leaves prior Gold. On backfill, serialize Silver replays, let them finish, rerun Gold and check old/new buckets before resuming.

**Measurements required in a provisioned environment:** log sample source epoch timestamps alongside Bronze arrival, Silver commit, successful SQL refresh and first analyst-visible Gold time over at least one live hour; calculate p50/p95/max source-to-landing/Silver/Gold lag and report p95 source-to-Gold <5 minutes as a *target*, not an observed guarantee. Record SQL run duration/failure rate, full-retained-Silver scan bytes, Gold partitions/files, warehouse uptime/billed DBUs and local regional SKU rates; quote actual spend only from billing. Two-minute cadence can incur idle/scan cost. No live measurements, verified analyst role or deployed Job are asserted by this repository.

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
