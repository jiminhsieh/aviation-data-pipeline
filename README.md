#  Raw Zone Transformer
This repository is used for the raw-to-silver data pipeline.

## Tech Stack
PySpark

## Core Logic
Read Avro format then flatten it and write it to silver zone.

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
