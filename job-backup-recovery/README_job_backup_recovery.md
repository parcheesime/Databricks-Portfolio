# Databricks Job and SQL Query Backup & Recovery

This folder contains four Databricks notebooks for backing up operational workspace metadata and recovering configuration when jobs or SQL queries need to be recreated.

The notebooks cover two related workflows:

1. **Databricks Jobs** — capture job settings from the Databricks REST API, store JSON backups in Amazon S3, maintain a lightweight Google Sheets inventory, and retrieve a saved job definition for recreation.
2. **Databricks SQL Queries** — retrieve saved SQL query definitions through the Databricks REST API, store timestamped JSON backups in S3, browse backups by creator, and recover SQL text for validation or recreation.

> The notebooks reference shared helper notebooks with `%run`. Those helpers are not included in this folder, so this README documents only the behavior visible in the four notebooks here.

## Repository Contents

### `NB_Databricks_Jobs_Backup.ipynb`

**Purpose:** Create a recurring backup of Databricks job configurations and a human-readable inventory of selected job metadata.

High-level flow:

1. Retrieve a Databricks API token from a Databricks secret scope.
2. Call shared helper functions to get the current list of jobs and retrieve each job's settings.
3. Save each job's settings as JSON to an S3 backup location.
4. Extract selected operational fields into a Pandas DataFrame, including:
   - job ID and name
   - schedule and timezone
   - notebook paths
   - cluster node type and Spark version
   - AWS instance profile
   - last-run URL
5. Call a shared Google Sheets helper to rotate the current and previous quick-view worksheets.

The S3 JSON files are the detailed recovery artifacts. The Google Sheet is a faster operational reference for comparing recent job configurations.

### `NB_Databricks_Jobs_Rebuild.ipynb`

**Purpose:** Locate a backed-up Databricks job definition in S3 and use its saved settings to recreate the job.

High-level flow:

1. Load Databricks API credentials and S3 configuration.
2. Load shared backup/rebuild helper functions.
3. List available S3 backup files so a desired job/date can be identified.
4. Retrieve the stored JSON for a selected job ID, optionally for a specific backup date.
5. Extract the saved `settings` object.
6. Pause the restored schedule before recreation.
7. Pass the saved settings to a helper that creates a new Databricks job through the API.

The actual recreation example is commented out in the public notebook, which makes the recovery process demonstrable without accidentally creating a job.

### `NB_Databricks_SQL_Queries_Backup.ipynb`

**Purpose:** Back up non-draft Databricks SQL query definitions to timestamped JSON files in S3.

High-level flow:

1. Load the Databricks API token and workspace configuration.
2. Call the Databricks SQL Queries API with pagination.
3. Calculate the number of pages from the API-reported query count.
4. Exclude draft queries from the backup set.
5. Normalize the query creator's name into an S3 folder name.
6. Serialize query metadata to formatted JSON.
7. Write timestamped JSON backups to S3 under creator-specific folders.

Example S3 organization:

```text
databricks-queries-settings-weekly/
  user_name/
    query_data_user_name_YYYY-MM-DD_HH-MM-SS-ffffff.json
```

### `NB_Databricks_SQL_Queries_Retrieval.ipynb`

**Purpose:** Browse backed-up query files, extract SQL text from the stored JSON, and test a recovered query in Spark SQL.

High-level flow:

1. Load workspace/S3 configuration and shared helper functions.
2. Browse the S3 backup directory and creator-specific subfolders.
3. Read multiline JSON backup files with Spark.
4. Select the stored `query` field and print SQL text for inspection.
5. Select a recovered query into `first_query`.
6. Execute the recovered SQL with `spark.sql(first_query)` and display the result.

This notebook is a retrieval and validation tool rather than an automated restore endpoint: it helps locate lost SQL text and confirm that the recovered query can run.

## Architecture

```text
                           DATABRICKS WORKSPACE
                     +---------------------------+
                     | Jobs     | SQL Queries    |
                     +----+-----------+----------+
                          |           |
                          | REST API  | REST API
                          v           v
              +----------------+   +----------------+
              | Jobs Backup    |   | SQL Query      |
              | Notebook       |   | Backup Notebook|
              +-------+--------+   +--------+-------+
                      |                     |
                      | JSON                | JSON
                      v                     v
              +-------------------------------------+
              |              Amazon S3              |
              | job settings | query definitions    |
              +-----------+-------------------------+
                          |                    |
                          v                    v
                +----------------+    +-------------------+
                | Jobs Rebuild   |    | Query Retrieval   |
                | Notebook       |    | Notebook          |
                +----------------+    +-------------------+
                          |
                          | selected metadata
                          v
                    Google Sheets
                    quick-view inventory
```

## Storage and Recovery Design

The design separates **durable machine-readable backups** from **human-readable operational visibility**:

- **S3 JSON backups** retain the detailed Databricks job/query configuration needed for recovery.
- **Timestamped files** preserve historical versions rather than only the latest state.
- **Creator-specific query folders** make SQL query backups easier to browse.
- **Google Sheets quick views** provide a lightweight inventory of current and previous job metadata without opening individual JSON files.
- **Databricks secret scopes** keep the API token out of notebook source code.

## Databricks and AWS Components

- Databricks notebooks
- Databricks REST APIs
- Databricks secret scopes (`dbutils.secrets`)
- Spark / PySpark for recovered query inspection and execution
- Amazon S3 for backup storage
- `boto3` for S3 writes
- `requests` for REST API calls
- Pandas for the job quick-view DataFrame
- Google Sheets integration through a shared helper notebook

## Code Review Notes

These notebooks are sanitized portfolio examples and contain a few items that would be cleaned up before reuse as a production package:

- The two job notebooks depend on `nb_functions_job_backup_rebuild`, and the backup notebook also depends on `nb_df_googlesheet`; those helper notebooks are not included here.
- The rebuild notebook's opening comment describes the backup workflow rather than the rebuild workflow.
- Sanitized S3 folder placeholders differ between the job backup and rebuild notebooks; a deployed version should use one shared configuration value.
- In the SQL backup notebook, the loop calls `save_query_data_to_s3(bucket, name, queried_data)`. As written, this writes the entire query list for every loop iteration rather than only the current `data` item. If the intended design is one query per file/creator, the function call should pass the current query or group queries by creator before writing.
- In the SQL retrieval notebook, the folder-listing cell loops with `for subfolder_name in file_list:` but prints `file.name`; that variable name should be corrected before execution.
- Several imports and variables are unused in the visible notebook code and could be removed during cleanup.

## Safety Considerations

Recovery code can create or execute workspace resources. The public notebooks reduce accidental side effects by keeping the job recreation example commented out and by making query execution an explicit final step after retrieval and inspection.
