#  Streaming Data Pipeline
This repository is used for the streaming data pipeline.

## Tech Stack
PySpark

## Core Logic
1. Ingest Kafka Avro records then archive Avro format of payloads to raw zone and write flattened datasets to sliver zone.
2. The partition should be day, hour, and minutes like `dt=2026-10-12/hr=23/min=59`. In this case, we can always use overwrite and rerun the older data.
3. Spark streaming interval should be set somewhere between 1 min to 5 mins, so the data latency could be within this range. 

## Source Data Spec 
1. https://openskynetwork.github.io/opensky-api/rest.html
2. [Data structure of the response in JSON format](example_response.json)
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
