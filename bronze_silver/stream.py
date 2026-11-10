"""Bronze Avro -> complete-hour Silver replacement on a single classic Jobs writer."""
import argparse
from datetime import datetime, timezone
import json
import re
import time

from pyspark.sql import functions as F, Window
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, LongType,
                               StringType, StructField, StructType)

# Spark 3.5 / DBR 15.4 decodes the producer's nullable heterogeneous item union
# as a nullable struct (null, string, long, double, boolean, array<long>).
BRANCHES = {"member0": StringType(), "member1": LongType(),
            "member2": DoubleType(), "member3": BooleanType(),
            "member4": ArrayType(LongType())}
UNION = StructType([StructField(name, dtype) for name, dtype in BRANCHES.items()])
RAW_SCHEMA = StructType([
    StructField("time", LongType()),
    StructField("states", ArrayType(ArrayType(UNION))),
])
COLUMNS = ["snapshot_time", "snapshot_at_utc", "snapshot_hour_utc", "bucket_start_utc",
           "icao24", "callsign", "origin_country", "time_position", "last_contact",
           "longitude", "latitude", "baro_altitude_m", "geo_altitude_m", "on_ground",
           "velocity_mps", "true_track_deg", "vertical_rate_mps", "squawk", "spi",
           "position_source", "sensors", "bronze_path", "processed_at_utc"]
PATH = re.compile(r"/year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/(\d+)\.avro$")
HORIZON_SECONDS = 30 * 86400


def audit(**values):
    # Job owner/operator access to driver logs must be restricted; failed output blocks acknowledgement.
    print(json.dumps(values, sort_keys=True), flush=True)


def hour_dir(root, hour):
    dt = datetime.fromtimestamp(hour, timezone.utc)
    return f"{root.rstrip('/')}/year={dt:%Y}/month={dt:%m}/day={dt:%d}/hour={dt:%H}"


def check_files(rows, now, expected_hours=None):
    """Check one record per discovered canonical path and source/path/retention agreement."""
    seen = set()
    hours = set()
    for row in rows:
        path, stamp = row.bronze_path, row.time
        if not isinstance(stamp, int) or stamp < 0:
            raise ValueError(f"invalid source time: {path}")
        try:
            dt = datetime.fromtimestamp(stamp, timezone.utc)
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError(f"invalid source time: {path}") from exc
        match = PATH.search(path or "")
        if not match or match.groups() != (dt.strftime("%Y"), dt.strftime("%m"),
                                            dt.strftime("%d"), dt.strftime("%H"), str(stamp)):
            raise ValueError(f"noncanonical source path/time: {path}")
        hour = stamp // 3600 * 3600
        if hour < now - HORIZON_SECONDS:
            raise ValueError(f"source hour outside retained Bronze horizon: {path}")
        if expected_hours is not None and hour not in expected_hours:
            raise ValueError(f"source file outside requested hour: {path}")
        if path in seen:
            raise ValueError(f"canonical file contains multiple records: {path}")
        seen.add(path)
        hours.add(hour)
    return hours, seen


def read_hours(spark, root, hours, discovered, now):
    paths = [hour_dir(root, hour) for hour in sorted(hours)]
    raw = (spark.read.format("avro").schema(RAW_SCHEMA).option("mode", "FAILFAST")
           .option("ignoreCorruptFiles", "false").option("ignoreMissingFiles", "false")
           .option("pathGlobFilter", "*.avro").load(paths)
           .select("*", F.col("_metadata.file_path").alias("bronze_path")))
    found, names = check_files(raw.select("time", "bronze_path").collect(), now, hours)
    if found != hours or not discovered <= names:
        raise ValueError("discovered file unreadable or affected hour has no canonical input")
    return raw


def silver_rows(raw):
    rows = raw.select("time", "bronze_path", F.posexplode("states").alias("state_index", "state"))
    # Reject malformed canonical records, not merely individual invalid observations.
    optional = {1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 13, 14}
    allowed = {0: {"member0"}, 1: {"member0"}, 2: {"member0"},
               3: {"member1"}, 4: {"member1"}, 5: {"member1", "member2"},
               6: {"member1", "member2"}, 7: {"member1", "member2"},
               8: {"member3"}, 9: {"member1", "member2"},
               10: {"member1", "member2"}, 11: {"member1", "member2"},
               12: {"member4"}, 13: {"member1", "member2"},
               14: {"member0"}, 15: {"member3"}, 16: {"member1"}}
    valid = F.col("state").isNotNull() & (F.size("state") == 17)
    for i, choices in allowed.items():
        item = F.col("state").getItem(i)
        members = {name: item.getField(name) for name in BRANCHES}
        count = sum([F.when(value.isNotNull(), 1).otherwise(0) for value in members.values()])
        selected = F.lit(False)
        for name in choices:
            selected = selected | members[name].isNotNull()
        valid = valid & F.when(item.isNull(), F.lit(i in optional)).otherwise((count == 1) & selected)
    malformed = rows.where(~valid).select("bronze_path", "state_index").first()
    if malformed:
        raise ValueError(f"malformed canonical state: {malformed.bronze_path} row {malformed.state_index}")

    def member(i, name):
        return F.col("state").getItem(i).getField(name)

    def number(i):
        return F.coalesce(member(i, "member2"), member(i, "member1").cast("double"))

    normalized = rows.select(
        F.col("time").alias("snapshot_time"),
        F.timestamp_seconds("time").alias("snapshot_at_utc"),
        F.timestamp_seconds(F.floor(F.col("time") / 3600) * 3600).alias("snapshot_hour_utc"),
        F.timestamp_seconds(F.floor(F.col("time") / 300) * 300).alias("bucket_start_utc"),
        F.lower(member(0, "member0")).alias("icao24"),
        F.trim(member(1, "member0")).alias("callsign"),
        member(2, "member0").alias("origin_country"),
        member(3, "member1").alias("time_position"),
        member(4, "member1").alias("last_contact"),
        number(5).alias("longitude"), number(6).alias("latitude"),
        number(7).alias("baro_altitude_m"), number(13).alias("geo_altitude_m"),
        member(8, "member3").alias("on_ground"),
        number(9).alias("velocity_mps"), number(10).alias("true_track_deg"),
        number(11).alias("vertical_rate_mps"), member(14, "member0").alias("squawk"),
        member(15, "member3").alias("spi"), member(16, "member1").alias("position_source"),
        member(12, "member4").alias("sensors"), "bronze_path", "state_index")
    for name in ("longitude", "latitude", "baro_altitude_m", "geo_altitude_m",
                 "velocity_mps", "true_track_deg", "vertical_rate_mps"):
        if normalized.where(F.isnan(name) | (F.abs(F.col(name)) == float("inf"))).limit(1).count():
            raise ValueError(f"non-finite canonical measurement: {name}")
    int_min, int_max = -(2**31), 2**31 - 1
    overflow = ((F.col("position_source") < int_min) | (F.col("position_source") > int_max) |
                F.coalesce(F.exists("sensors", lambda x: (x < int_min) | (x > int_max)), F.lit(False)))
    if normalized.where(overflow).limit(1).count():
        raise ValueError("canonical INT measurement outside Silver range")
    normalized = normalized.withColumn("position_source", F.col("position_source").cast("int"))
    normalized = normalized.withColumn("sensors", F.col("sensors").cast("array<int>"))
    reason = (F.when(~F.col("icao24").rlike("^[0-9a-f]{6}$"), "invalid_icao24")
              .when(((F.col("longitude") < -180) | (F.col("longitude") > 180) |
                     (F.col("latitude") < -90) | (F.col("latitude") > 90)), "invalid_coordinates"))
    normalized = normalized.withColumn("reject_reason", reason)
    rejected = normalized.where(F.col("reject_reason").isNotNull())
    rejected_count = 0
    for row in rejected.select("bronze_path", "state_index", "reject_reason").collect():
        audit(event="silver_reject", scope="complete_hour_rebuild", bronze_path=row.bronze_path,
              state_index=row.state_index, reason=row.reject_reason)
        rejected_count += 1
    good = normalized.where(F.col("reject_reason").isNull())
    content = [c for c in COLUMNS if c not in ("bronze_path", "processed_at_utc")]
    good = good.withColumn("content_hash", F.sha2(F.to_json(F.struct(*content), {"ignoreNullFields": "false"}), 256))
    rank = Window.partitionBy("snapshot_time", "icao24").orderBy(
        F.col("last_contact").desc_nulls_last(), F.col("time_position").desc_nulls_last(),
        F.col("content_hash").desc(), F.col("bronze_path").asc(), F.col("state_index").asc())
    good = good.withColumn("rank", F.row_number().over(rank)).where("rank = 1")
    return good.withColumn("processed_at_utc", F.current_timestamp()).select(*COLUMNS), rejected_count


def rebuild(spark, source_root, table, hours, discovered=(), now=None):
    if not hours:
        return
    now = time.time() if now is None else now
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "false")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "false")
    spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", "true")
    raw = read_hours(spark, source_root, set(hours), set(discovered), now)
    silver, rejected = silver_rows(raw)
    values = [datetime.fromtimestamp(h, timezone.utc).strftime("%Y-%m-%d %H:%M:%S") for h in sorted(hours)]
    predicate = "snapshot_hour_utc IN (" + ", ".join(f"TIMESTAMP '{v}'" for v in values) + ")"
    count = silver.count()
    # An empty DataFrame with replaceWhere clears the selected hour; never skip this write.
    (silver.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
     .saveAsTable(table))
    audit(event="silver_commit", scope="complete_hour_rebuild", hours=values,
          silver_rows=count, rejected_rows=rejected)


def process_batch(batch, batch_id, source_root, table):
    rows = batch.select("time", "bronze_path").collect()
    if not rows:
        return
    now = time.time()
    hours, discovered = check_files(rows, now)
    rebuild(batch.sparkSession, source_root, table, hours, discovered, now)


def main():
    parser = argparse.ArgumentParser(description="One-writer Bronze to Silver stream or bounded replay")
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--replay-from", type=int)
    parser.add_argument("--replay-to", type=int)
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if (args.replay_from is None) != (args.replay_to is None):
        parser.error("provide both replay bounds; pause and verify the normal Job is stopped first")
    if args.replay_from is not None:
        if args.replay_from < 0 or args.replay_from > args.replay_to:
            parser.error("invalid replay interval")
        first, last = args.replay_from // 3600 * 3600, args.replay_to // 3600 * 3600
        if last - first > HORIZON_SECONDS or first < time.time() - HORIZON_SECONDS:
            parser.error("replay interval outside retained 30-day Bronze horizon")
        rebuild(spark, args.source_root, args.table, range(first, last + 3600, 3600))
        return
    stream = (spark.readStream.format("avro").schema(RAW_SCHEMA).option("mode", "FAILFAST")
              .option("ignoreCorruptFiles", "false").option("ignoreMissingFiles", "false")
              .option("recursiveFileLookup", "true").option("pathGlobFilter", "*.avro")
              .option("maxFilesPerTrigger", 32).load(args.source_root)
              .select("*", F.col("_metadata.file_path").alias("bronze_path")))
    query = (stream.writeStream.foreachBatch(
        lambda batch, batch_id: process_batch(batch, batch_id, args.source_root, args.table))
        .option("checkpointLocation", args.checkpoint).trigger(processingTime="1 minute").start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
