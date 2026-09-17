# Databricks Event Stream

This folder contains a small Databricks streaming pipeline for application user-event logs. The two notebooks work together:

- `DDL_event_stream_logs.ipynb` defines the Delta destination table and its output schema.
- `NB_Pipeline_event_stream.ipynb` uses Databricks Auto Loader and Spark Structured Streaming to ingest nested JSON event files, flatten them into one row per event, attach operational metadata, and write the results to Delta.

## Data flow

```text
Application event JSON files
        |
        v
Mounted raw-data directory
        |
        v
Databricks Auto Loader (`cloudFiles`)
        |
        v
Explicit nested Spark schema
        |
        +--> source file metadata
        +--> file modification date/time
        +--> processing timestamp
        |
        v
`explode(Event)`
        |
        v
Flatten event fields
        |
        v
Delta output in S3
        |
        +--> Structured Streaming checkpoint
```

## Destination table

`DDL_event_stream_logs.ipynb` creates the Delta table used by the pipeline.

| Column                     | Purpose                                             |
| -------------------------- | --------------------------------------------------- |
| `cusip`                  | Security identifier associated with the event       |
| `org`                    | Organization associated with the user               |
| `role`                   | User role                                           |
| `source`                 | Application/source context                          |
| `user`                   | User identifier                                     |
| `file_modification_date` | Date derived from source-file modification metadata |
| `file_modification_time` | Time derived from source-file modification metadata |
| `utc_processing_time`    | Timestamp added when Spark processes the record     |
| `path`                   | Application path contained in the event             |
| `file_name`              | Physical source file name                           |
| `file_path`              | Physical source file path                           |

The table uses Delta format and points to a configured data location.

## Streaming pipeline

### 1. Define the source schema

The source JSON contains an outer `Event` array. Each array element contains:

- `cusip`
- `organization`
- `path`
- `role`
- `source`
- `user`

The notebook defines this structure explicitly with `StructType`, `StructField`, and `ArrayType` rather than relying on schema inference.

### 2. Read new JSON files with Auto Loader

The pipeline starts a streaming read with:

```python
spark.readStream.format("cloudFiles")
```

Important options in the notebook:

- `cloudFiles.format = json` tells Auto Loader to ingest JSON files.
- `wholeText = true` treats each file as a complete JSON document.
- `ignoreMissingFiles = true` allows the read to tolerate files that disappear between discovery and access.
- `.schema(finalSchema)` applies the explicit nested schema.

### 3. Capture source-file metadata

The streaming DataFrame also selects:

- `_metadata.file_path`
- `_metadata.file_name`
- `_metadata.file_modification_time`

The pipeline derives a date and formatted time from the file modification timestamp and adds `utc_processing_time` with `current_timestamp()`.

Keeping both source-file timing and processing timing makes it possible to distinguish when a file was modified from when the pipeline processed it.

### 4. Explode and flatten events

The source contains an array of events, so:

```python
F.explode("Event")
```

creates one Spark row per event.

The next `select` flattens the nested event struct and renames `organization` to `org` so the streaming output matches the DDL schema.

### 5. Write to Delta

The final DataFrame is written with `writeStream.format("delta")` to the configured S3 data location. A checkpoint location is supplied so Spark Structured Streaming can persist progress and resume after a restart.

## Why the two notebooks are paired

The DDL notebook defines the downstream contract. The pipeline notebook transforms the nested source data into exactly that shape before writing it. Together they separate:

1. **Table/schema definition**
2. **Streaming ingestion and transformation logic**

## Code-review notes

The sanitized notebook currently contains a few cleanup items worth knowing:

- `todaysDate` is defined, while the checkpoint path references `todays_date`. Those names should be made consistent.
- `cloudFiles.inferColumnTypes` appears on the streaming writer. That is an Auto Loader/read-side option and is unnecessary when an explicit schema is already supplied.
- `LongType`, `TimestampType`, and `boto3` are imported but are not used in the current notebook.
- The commented Delta table properties show that optimized writes and auto-compaction were considered, but they are not enabled in the notebook as shown.

These notes describe the sanitized portfolio version and are not claims about any separate production deployment.
