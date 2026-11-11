-- One atomic replacement from all retained Silver, including when Silver is empty.
SET TIME ZONE 'UTC';

INSERT OVERWRITE TABLE IDENTIFIER(:catalog || '.gold.aircraft_activity_5m')
  (bucket_hour_utc, bucket_start_utc, origin_country, aircraft_count,
   avg_baro_altitude_m, altitude_sample_count, last_source_time, updated_at_utc)
WITH per_aircraft AS (
  SELECT bucket_start_utc, icao24,
         MAX_BY(origin_country, snapshot_time) AS origin_country,
         MAX_BY(baro_altitude_m, snapshot_time) FILTER (
           WHERE baro_altitude_m IS NOT NULL
         ) AS latest_altitude_m,
         MAX(snapshot_time) AS last_source_time
  FROM IDENTIFIER(:catalog || '.silver.aircraft_states')
  GROUP BY bucket_start_utc, icao24
)
SELECT DATE_TRUNC('HOUR', bucket_start_utc), bucket_start_utc,
       COALESCE(NULLIF(TRIM(origin_country), ''), 'Unknown'),
       COUNT(*), AVG(latest_altitude_m), COUNT(latest_altitude_m),
       MAX(last_source_time), CURRENT_TIMESTAMP()
FROM per_aircraft
GROUP BY bucket_start_utc, COALESCE(NULLIF(TRIM(origin_country), ''), 'Unknown');
