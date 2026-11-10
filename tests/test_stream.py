"""Local Spark 3.5/Delta 3.2 contract checks; run pinned DBR integration before deployment."""
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from delta import configure_spark_with_delta_pip
from fastavro import reader, writer
from pyspark.sql import Row, SparkSession, functions as F

from bronze_silver import stream

FIXTURE = Path(__file__).parent / "fixtures/1793013825.avro"  # produced by ingestion/collector.py encode
SAMPLE = json.loads((Path(__file__).parent.parent / "example_response.json").read_text())
with FIXTURE.open("rb") as handle:
    SCHEMA = reader(handle).writer_schema
NOW = SAMPLE["time"] + 86400


class SilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sandbox = tempfile.TemporaryDirectory()
        cls.spark = configure_spark_with_delta_pip(
            SparkSession.builder.master("local[2]").appName("silver-contract")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", cls.sandbox.name + "/warehouse")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            extra_packages=["org.apache.spark:spark-avro_2.12:3.5.0"]
        ).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.sandbox.cleanup()

    def setUp(self):
        self.root = Path(self.sandbox.name) / self._testMethodName
        self.root.mkdir()
        self.table = "silver_" + self._testMethodName
        self.add(SAMPLE, from_producer=True)

    def tearDown(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table}")

    def add(self, record, from_producer=False):
        stamp = record["time"]
        directory = Path(stream.hour_dir(str(self.root), stamp // 3600 * 3600))
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{stamp}.avro"
        if from_producer:
            shutil.copyfile(FIXTURE, target)
        else:
            with target.open("wb") as handle:
                writer(handle, SCHEMA, [record], codec="snappy")
        return target

    def raw(self):
        return (self.spark.read.format("avro").schema(stream.RAW_SCHEMA)
                .option("recursiveFileLookup", "true").load(str(self.root))
                .select("*", F.col("_metadata.file_path").alias("bronze_path")))

    def create_table(self):
        silver, _ = stream.silver_rows(self.raw())
        silver.limit(0).write.format("delta").partitionBy("snapshot_hour_utc").saveAsTable(self.table)

    def rebuild(self, hours):
        with patch.object(stream.time, "time", return_value=NOW):
            stream.rebuild(self.spark, str(self.root), self.table, hours)

    def test_producer_union_and_optional_fields(self):
        self.create_table()
        stamp = SAMPLE["time"]
        self.rebuild({stamp // 3600 * 3600})
        row = self.spark.table(self.table).first()
        self.assertEqual((row.snapshot_time, row.icao24, row.callsign, row.origin_country),
                         (stamp, "8a044f", "GIA190", "Indonesia"))
        self.assertEqual((row.longitude, row.latitude, row.baro_altitude_m, row.sensors),
                         (99.9767, 1.7853, 10927.08, None))
        self.assertFalse(row.on_ground)
        self.assertFalse(row.spi)
        self.assertIsNone(row.squawk)
        self.assertEqual(row.position_source, 0)
        self.assertTrue(row.bronze_path.endswith("/1793013825.avro"))
        hour = self.spark.table(self.table).select(F.date_format("snapshot_hour_utc", "yyyy-MM-dd HH")).first()[0]
        self.assertEqual(hour, "2026-10-26 11")

    def test_nulls_sensors_numeric_and_boolean_union_branches(self):
        self.create_table()
        state = SAMPLE["states"][0][:]
        state[0], state[1], state[2], state[5], state[6], state[7] = "ABCDEF", None, "", 100, None, None
        state[8], state[12], state[16] = True, [1, 2], 2
        self.add({"time": SAMPLE["time"] + 2, "states": [state]})
        self.rebuild({SAMPLE["time"] // 3600 * 3600})
        row = self.spark.table(self.table).where("icao24 = 'abcdef'").first()
        self.assertIsNone(row.callsign)
        self.assertIsNone(row.latitude)
        self.assertIsNone(row.baro_altitude_m)
        self.assertEqual(row.origin_country, "")  # Gold alone maps blank to Unknown.
        self.assertEqual((row.longitude, row.sensors, row.position_source, row.on_ground),
                         (100.0, [1, 2], 2, True))

    def test_complete_hour_late_files_multi_hour_and_retry(self):
        self.create_table()
        hour = SAMPLE["time"] // 3600 * 3600
        self.rebuild({hour})
        other = {"time": SAMPLE["time"] + 70, "states": [SAMPLE["states"][0][:]]}
        other["states"][0][1] = "NEW    "
        self.add(other)
        late = {"time": SAMPLE["time"] + 3700, "states": [SAMPLE["states"][0][:]]}
        late["states"][0][0] = "aabbcc"
        self.add(late)
        self.rebuild({hour, late["time"] // 3600 * 3600})
        self.rebuild({hour})  # commit succeeded but acknowledgement failed: same business keys
        rows = self.spark.table(self.table).select("snapshot_time", "icao24").collect()
        self.assertEqual(set((r.snapshot_time, r.icao24) for r in rows),
                         {(SAMPLE["time"], "8a044f"), (other["time"], "8a044f"),
                          (late["time"], "aabbcc")})

    def test_rejects_dedup_and_accounting_failure(self):
        self.create_table()
        hour = SAMPLE["time"] // 3600 * 3600
        newer = {"time": SAMPLE["time"] + 1, "states": [SAMPLE["states"][0][:]]}
        same = newer["states"][0][:]
        same[4] += 1
        same[1] = "WINNER "
        newer["states"].append(same)
        invalid = same[:]
        invalid[0] = "not-icao"
        newer["states"].append(invalid)
        invalid_coord = same[:]
        invalid_coord[0] = "abcdef"
        invalid_coord[5] = 181.0
        newer["states"].append(invalid_coord)
        self.add(newer)
        with patch.object(stream, "audit", side_effect=OSError("accounting unavailable")):
            with self.assertRaisesRegex(OSError, "accounting unavailable"):
                self.rebuild({hour})
        self.assertEqual(self.spark.table(self.table).count(), 0)
        self.rebuild({hour})
        rows = {r.snapshot_time: r for r in self.spark.table(self.table).collect()}
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[newer["time"]].callsign, "WINNER")

    def test_empty_replacement_and_malformed_fail_closed(self):
        self.create_table()
        hour = SAMPLE["time"] // 3600 * 3600
        self.rebuild({hour})
        # Seed a second Silver hour, then rebuild it from its only, empty Bronze snapshot.
        empty = {"time": SAMPLE["time"] + 3700, "states": []}
        self.add(empty)
        next_hour = empty["time"] // 3600 * 3600
        seed = (self.spark.table(self.table)
                .withColumn("snapshot_time", F.lit(empty["time"]))
                .withColumn("snapshot_at_utc", F.timestamp_seconds(F.lit(empty["time"])))
                .withColumn("snapshot_hour_utc", F.timestamp_seconds(F.lit(next_hour)))
                .withColumn("bucket_start_utc", F.timestamp_seconds(F.lit(empty["time"] // 300 * 300))))
        seed.write.mode("append").insertInto(self.table)
        self.assertEqual(self.spark.table(self.table).count(), 2)
        self.rebuild({next_hour})
        self.assertEqual(self.spark.table(self.table).count(), 1)  # unrelated hour untouched
        invalid = {"time": SAMPLE["time"] + 70, "states": [["short"]]}
        self.add(invalid)
        with self.assertRaisesRegex(ValueError, "malformed canonical state"):
            self.rebuild({hour})
        self.assertEqual(self.spark.table(self.table).count(), 1)
        self.assertEqual(len(stream.check_files([], NOW)[0]), 0)

    def test_commit_before_acknowledgement_and_bounded_replay(self):
        self.create_table()
        hour = SAMPLE["time"] // 3600 * 3600
        with patch.object(stream, "audit", side_effect=OSError("audit unavailable")):
            with self.assertRaisesRegex(OSError, "audit unavailable"):
                self.rebuild({hour})
        self.assertEqual(self.spark.table(self.table).count(), 1)  # Delta commit succeeded
        self.rebuild({hour})  # retry without a checkpoint acknowledgement
        before = self.spark.table(self.table).select("snapshot_time", "icao24", "callsign").collect()
        argv = ["stream", "--source-root", str(self.root), "--checkpoint", "unused-by-replay",
                "--table", self.table, "--replay-from", str(SAMPLE["time"]),
                "--replay-to", str(SAMPLE["time"])]
        with patch("sys.argv", argv), patch.object(stream.time, "time", return_value=NOW):
            stream.main()
        after = self.spark.table(self.table).select("snapshot_time", "icao24", "callsign").collect()
        self.assertEqual(before, after)

    def test_checkpoint_restart_and_new_hour_discovery(self):
        self.create_table()
        checkpoint = str(self.root.parent / (self._testMethodName + "_checkpoint"))
        root, table = str(self.root), self.table

        def start_once():
            source = (self.spark.readStream.format("avro").schema(stream.RAW_SCHEMA)
                      .option("recursiveFileLookup", "true").option("pathGlobFilter", "*.avro")
                      .load(root)
                      .select("*", F.col("_metadata.file_path").alias("bronze_path")))
            query = (source.writeStream.foreachBatch(
                lambda batch, batch_id: stream.process_batch(batch, batch_id, root, table))
                .option("checkpointLocation", checkpoint).trigger(availableNow=True).start())
            query.awaitTermination(90)
            self.assertIsNone(query.exception())

        with patch.object(stream.time, "time", return_value=NOW):
            start_once()
            later = {"time": SAMPLE["time"] + 3700, "states": [SAMPLE["states"][0][:]]}
            self.add(later)
            start_once()  # original checkpoint: only the newly discovered hour is processed
            before = self.spark.table(self.table).select("snapshot_time", "icao24", "processed_at_utc").collect()
            start_once()
        after = self.spark.table(self.table).select("snapshot_time", "icao24", "processed_at_utc").collect()
        self.assertEqual(set(tuple(r) for r in before), set(tuple(r) for r in after))
        self.assertEqual({r.snapshot_time for r in after}, {SAMPLE["time"], later["time"]})

    def test_expired_and_mismatched_source_path(self):
        hour = SAMPLE["time"] // 3600 * 3600
        original = Row(time=SAMPLE["time"], bronze_path=str(next(self.root.rglob("*.avro"))))
        with self.assertRaisesRegex(ValueError, "outside retained Bronze horizon"):
            stream.check_files([original], SAMPLE["time"] + 31 * 86400)
        wrong = Row(time=SAMPLE["time"], bronze_path=str(self.root / f"{SAMPLE['time'] + 1}.avro"))
        with self.assertRaisesRegex(ValueError, "noncanonical source path/time"):
            stream.check_files([wrong], NOW, {hour})
        self.create_table()
        self.rebuild(set())
        self.assertEqual(self.spark.table(self.table).count(), 0)

    def test_path_discovery_and_loss(self):
        self.create_table()
        raw = self.raw()
        with patch.object(stream.time, "time", return_value=NOW):
            stream.process_batch(raw, 0, str(self.root), self.table)
        self.assertEqual(self.spark.table(self.table).count(), 1)
        file = next(self.root.rglob("*.avro"))
        file.unlink()
        with patch.object(stream.time, "time", return_value=NOW):
            with self.assertRaises(Exception):
                stream.process_batch(raw, 1, str(self.root), self.table)
        self.assertEqual(self.spark.table(self.table).count(), 1)


if __name__ == "__main__":
    unittest.main()
