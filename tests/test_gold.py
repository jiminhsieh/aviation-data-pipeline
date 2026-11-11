"""Run the versioned Gold SQL against local Spark 3.5/Delta 3.2 fixtures."""
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

SQL = (Path(__file__).parent.parent / "sql/silver_to_gold.sql").read_text()
START = 1793058900  # 2026-10-26 23:55:00 UTC


def stamp(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc)


def bucket(seconds):
    return stamp(seconds).strftime("%Y-%m-%d %H:%M:%S")


class GoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sandbox = tempfile.TemporaryDirectory()
        cls.spark = configure_spark_with_delta_pip(
            SparkSession.builder.master("local[2]").appName("gold-contract")
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
        self.silver = "silver_" + self._testMethodName
        self.gold = "gold_" + self._testMethodName
        self.spark.sql(f"""CREATE TABLE {self.silver} (
            snapshot_time BIGINT, bucket_start_utc TIMESTAMP, icao24 STRING,
            origin_country STRING, baro_altitude_m DOUBLE) USING DELTA""")
        self.spark.sql(f"""CREATE TABLE {self.gold} (
            bucket_hour_utc TIMESTAMP, bucket_start_utc TIMESTAMP, origin_country STRING,
            aircraft_count BIGINT, avg_baro_altitude_m DOUBLE, altitude_sample_count BIGINT,
            last_source_time BIGINT, updated_at_utc TIMESTAMP)
            USING DELTA PARTITIONED BY (bucket_hour_utc)""")

    def tearDown(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.silver}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.gold}")

    def add(self, *rows):
        # Each fixture: (seconds after START, ICAO24, country, altitude).
        data = [(START + offset, icao, country, altitude)
                for offset, icao, country, altitude in rows]
        (self.spark.createDataFrame(data, """snapshot_time LONG, icao24 STRING,
            origin_country STRING, baro_altitude_m DOUBLE""")
         .select("snapshot_time",
                 F.timestamp_seconds(F.col("snapshot_time") - F.col("snapshot_time") % 300)
                 .alias("bucket_start_utc"), "icao24", "origin_country", "baro_altitude_m")
         .write.insertInto(self.silver))

    def refresh(self, source=None):
        # Substitute only the catalog-qualified table bindings; execute the actual SQL file.
        sql = SQL.replace("IDENTIFIER(:catalog || '.gold.aircraft_activity_5m')", self.gold)
        sql = sql.replace("IDENTIFIER(:catalog || '.silver.aircraft_states')", source or self.silver)
        self.assertNotIn(":catalog", sql)
        for statement in sql.split(";"):
            if statement.strip():
                self.spark.sql(statement)

    def rows(self):
        return {(r.bucket, r.origin_country): r for r in self.spark.table(self.gold)
                .selectExpr("date_format(bucket_start_utc, 'yyyy-MM-dd HH:mm:ss') AS bucket",
                            "date_format(bucket_hour_utc, 'yyyy-MM-dd HH:mm:ss') AS hour",
                            "origin_country", "aircraft_count", "avg_baro_altitude_m",
                            "altitude_sample_count", "last_source_time", "updated_at_utc").collect()}

    def test_latest_country_per_aircraft_and_within_bucket_altitude(self):
        self.add((1, "a", "Indonesia", 100.0), (50, "a", "Sweden", None),
                 (2, "b", "Sweden", 400.0), (240, "b", "Sweden", 200.0),
                 (5, "c", " ", None), (6, "c", None, None),
                 (7, "d", "Indonesia", 60.0),
                 (301, "d", "Indonesia", None), (302, "a", "Indonesia", 25.0))
        self.refresh()
        rows = self.rows()
        sweden = rows[(bucket(START), "Sweden")]
        self.assertEqual((sweden.aircraft_count, sweden.avg_baro_altitude_m,
                          sweden.altitude_sample_count, sweden.last_source_time),
                         (2, 150.0, 2, START + 240))
        self.assertEqual((rows[(bucket(START), "Unknown")].aircraft_count,
                          rows[(bucket(START), "Unknown")].altitude_sample_count), (1, 0))
        self.assertIsNone(rows[(bucket(START), "Unknown")].avg_baro_altitude_m)
        self.assertEqual(rows[(bucket(START), "Indonesia")].avg_baro_altitude_m, 60.0)
        next_bucket = rows[(bucket(START + 300), "Indonesia")]
        self.assertEqual((next_bucket.aircraft_count, next_bucket.altitude_sample_count,
                          next_bucket.avg_baro_altitude_m), (2, 1, 25.0))
        self.assertEqual(next_bucket.hour, bucket(START + 300))
        self.assertEqual(rows[(bucket(START), "Sweden")].hour, "2026-10-26 23:00:00")
        self.assertEqual(len(rows), 4)

    def test_retry_late_correction_obsolete_group_hour_and_empty_source(self):
        self.add((1, "a", "Old", 10.0), (301, "b", "Late", 20.0))
        self.refresh()
        first = self.rows()
        self.refresh()  # unchanged input cannot inflate counts
        self.assertEqual({key: r.aircraft_count for key, r in self.rows().items()},
                         {key: r.aircraft_count for key, r in first.items()})
        self.add((2, "a", "New", 30.0))  # late correction to an earlier hour
        self.refresh()
        rows = self.rows()
        self.assertNotIn((bucket(START), "Old"), rows)
        self.assertEqual(rows[(bucket(START), "New")].avg_baro_altitude_m, 30.0)
        self.assertIn((bucket(START + 300), "Late"), rows)
        self.spark.sql(f"DELETE FROM {self.silver} WHERE snapshot_time >= {START + 300}")
        self.refresh()
        self.assertNotIn((bucket(START + 300), "Late"), self.rows())
        self.assertEqual(len(self.rows()), 1)
        self.spark.sql(f"DELETE FROM {self.silver}")
        self.refresh()
        self.assertEqual(self.rows(), {})
        self.assertEqual(self.spark.catalog.listColumns(self.gold)[0].name, "bucket_hour_utc")

    def test_failed_sql_preserves_gold_and_silver_progress(self):
        self.add((1, "a", "Original", 5.0))
        self.refresh()
        before = self.rows()
        with self.assertRaises(Exception):
            self.refresh("nonexistent_silver_source")
        self.assertEqual({key: r.aircraft_count for key, r in self.rows().items()},
                         {key: r.aircraft_count for key, r in before.items()})
        self.add((2, "b", "Independent", 6.0))  # Silver can still accept progress.
        self.assertEqual(self.spark.table(self.silver).count(), 2)
        self.refresh()
        self.assertEqual(len(self.rows()), 2)


if __name__ == "__main__":
    unittest.main()
