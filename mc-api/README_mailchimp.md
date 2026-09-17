# Mailchimp Data Collection Pipeline

## Overview

This Databricks notebook collects Mailchimp audience and campaign activity data through the Mailchimp Marketing API, normalizes the API responses, converts the results into Spark DataFrames, and persists curated datasets in Delta tables for downstream reporting and analysis.

The notebook covers several related Mailchimp entities rather than a single endpoint. It retrieves audience members, campaign summaries, campaign reports, open activity, click activity, unsubscribe activity, and member-level event history. It also includes a supplemental enrichment step that updates member classification data from a CSV source.

At a high level, the flow is:

```text
Mailchimp Marketing API
        |
        v
Paginated Python collection
        |
        v
Python lists / dictionaries
        |
        v
Pandas DataFrames
        |
        v
Spark DataFrames
        |
        v
Delta tables
        |
        +--> MERGE-based updates for campaign/activity datasets
        +--> OPTIMIZE / ZORDER for selected tables
```

## Notebook Flow

### 1. Client Configuration

The notebook initializes the Mailchimp Marketing client and selects the target audience using its Mailchimp audience/list ID.

```python
client = MailchimpMarketing.Client()
client.set_config({
    "api_key": "API_KEY",
    "server": "us10"
})
```

The public notebook uses placeholder credentials rather than exposing production secrets.

---

### 2. Audience Member Collection

The first major section retrieves members from the selected Mailchimp audience.

The API is paginated using `count` and `offset`. Each page is appended to `all_members` until the API returns no additional members.

```python
while True:
    response = client.lists.get_list_members_info(
        list_id,
        count=limit,
        offset=offset
    )

    members = response["members"]

    if members:
        all_members.extend(members)
        offset += limit
    else:
        break
```

Selected member fields are flattened from the nested API response, including:

- email and member identifiers
- name and contact fields
- firm name and classification
- signup and opt-in timestamps
- subscription status
- average open and click rates

The resulting Python records are converted first to a Pandas DataFrame and then to a Spark DataFrame before being written to Delta.

```text
Mailchimp member JSON
        -> Python dictionaries
        -> Pandas DataFrame
        -> Spark DataFrame
        -> Delta table
```

The member table in this notebook is written using append mode.

---

### 3. Campaign Summary Collection

The notebook retrieves all campaigns using API pagination and filters them to the selected audience.

Campaign-level fields include:

- campaign ID and title
- audience ID and name
- subject line
- creation and send timestamps
- opens and unique opens
- open rate
- clicks and subscriber clicks
- click rate

Optional fields are handled with `try` / `except` so missing values do not terminate the collection process.

The campaign records are converted into a Spark DataFrame and registered as a temporary view.

A Delta SQL `MERGE` then updates existing campaign rows or inserts new campaigns using the campaign ID as the business key.

```sql
ON target.id = source.id
```

This supports repeated pipeline runs without blindly inserting duplicate campaign records.

---

### 4. Campaign Report Collection

A second Mailchimp endpoint retrieves detailed campaign report data.

The report dataset includes metrics such as:

- emails sent
- abuse reports
- unsubscribes
- opens and unique opens
- clicks and unique clicks
- engagement rates
- bounce and unsubscribe rates

The records follow the same general pattern:

```text
API -> normalized records -> Pandas -> Spark -> temporary view -> Delta MERGE
```

The campaign report table is merged using campaign/report ID.

---

### 5. Campaign Open Details

For each campaign ID, the notebook retrieves member-level open details.

The response contains nested member information, so the notebook extracts and flattens fields such as:

- campaign ID
- email ID and address
- contact status
- number of opens
- first and last name
- address and phone
- firm name and firm class

Missing merge fields are handled individually so incomplete Mailchimp profiles do not stop the pipeline.

The Delta `MERGE` uses a composite key:

```sql
ON target.campaign_id = source.campaign_id
AND target.email_id = source.email_id
```

This represents one recipient's open record within a particular campaign.

---

### 6. Campaign Click Details

The notebook retrieves URL-level click information for each campaign.

Fields include:

- URL ID
- URL
- total clicks
- unique clicks
- click percentages
- last click timestamp
- campaign ID

The Delta merge uses both the URL/event identifier and campaign ID.

```sql
ON target.id = source.id
AND target.campaign_id = source.campaign_id
```

---

### 7. Unsubscribe Details

Campaign-level unsubscribe records are collected and normalized into a separate dataset.

The table retains both campaign context and recipient information, including the unsubscribe reason and timestamp.

The merge key is:

```sql
ON target.email_id = source.email_id
AND target.campaign_id = source.campaign_id
```

This keeps unsubscribe activity associated with both the subscriber and the campaign that generated it.

---

### 8. Member Activity

Mailchimp's member activity endpoint requires a subscriber hash derived from the lowercase email address.

```python
subscriber_hashes = [
    hashlib.md5(email.lower().encode()).hexdigest()
    for email in emails
]
```

The notebook retrieves activity for each subscriber and creates a Spark DataFrame containing fields such as:

- action
- timestamp
- campaign ID
- title
- email ID
- URL

The Spark DataFrame is deduplicated with `distinct()` before being merged into Delta.

The merge uses a multi-column event key:

```sql
ON target.email_id = source.email_id
AND target.timestamp = source.timestamp
AND target.campaign_id = source.campaign_id
AND target.url = source.url
```

This provides a more specific identity for individual member activity events.

---

### 9. Delta Optimization

Several curated Delta tables are followed by `OPTIMIZE ... ZORDER BY` operations.

Examples include ordering around common campaign, timestamp, email, action, and URL fields.

The intent is to improve data layout for frequently accessed fields in downstream analysis.

---

### 10. Supplemental Member Enrichment

The final section demonstrates enrichment from an external CSV file.

The notebook:

1. Reads the existing Mailchimp member Delta table.
2. Reads a CSV containing supplemental firm classification data.
3. Renames the incoming columns to avoid collisions.
4. Performs a left join on email address.
5. Uses `coalesce()` to prefer the supplemental firm classification when it exists while retaining the original value otherwise.
6. Adds an `updated_flag` to identify changed rows.
7. Writes the enriched result to a Delta table.

The update is performed with native Spark DataFrame functions rather than a Python UDF.

```python
updated_df = joined_df.withColumn(
    "FIRMCLASS",
    F.coalesce(joined_df["df1_firmclass"], joined_df["FIRMCLASS"])
)
```

## Data Engineering Concepts Demonstrated

This notebook demonstrates several common data-engineering patterns:

- REST API ingestion
- API pagination
- nested JSON normalization
- defensive handling of missing API fields
- Python and Pandas for small API response manipulation
- conversion from Pandas to Spark DataFrames
- Spark DataFrame transformations
- temporary SQL views
- Delta Lake storage
- Delta `MERGE` upserts
- composite business keys
- deduplication
- enrichment joins
- native Spark functions such as `coalesce`
- Delta `OPTIMIZE` and `ZORDER`

## Current Architecture Note

The notebook does not currently implement a formal Bronze/Silver/Gold separation. It focuses on API collection, normalization, and persistence into curated Delta tables.

A more explicit medallion design would first preserve the raw Mailchimp API responses in a Bronze layer and then perform normalization, deduplication, and merging in downstream Silver tables. That would provide stronger replayability, lineage, debugging, and recovery if transformation logic changed later.

## What I Would Update Today

This notebook reflects an earlier implementation of the pipeline. If I were redesigning it today, I would keep the useful business logic while making the pipeline more modular, incremental, and operationally robust.

### Add a Raw Bronze Layer

Persist the original API responses before transformation.

```text
Mailchimp API
    -> Bronze: raw JSON + ingestion metadata
    -> Silver: normalized members / campaigns / activity
    -> Gold: reporting and KPI datasets
```

This would preserve source data for replay, auditing, and schema-change troubleshooting.

### Make Extraction Incremental

The current notebook can retrieve broad historical collections on each run. I would investigate Mailchimp timestamps or other supported filters and persist pipeline state so routine runs only retrieve records that are new or have changed.

### Consolidate API Pagination and Error Handling

Several sections repeat the same `offset` / `limit` loop. I would move that behavior into reusable collection helpers with:

- pagination
- retry/backoff behavior
- structured logging
- consistent exception handling
- API-rate-limit handling

### Separate Pipeline Responsibilities

Rather than one large notebook, I would split the workflow into smaller independently testable tasks, for example:

```text
collect_members
collect_campaigns
collect_reports
collect_activity
normalize
merge_curated_tables
enrich_member_data
quality_checks
```

Those tasks could then be orchestrated as a Databricks job with explicit dependencies.

### Use Explicit Schemas Where Practical

The notebook often relies on Pandas followed by `spark.createDataFrame()` to infer Spark types. For production datasets I would define explicit Spark schemas where practical so schema behavior is predictable across runs.

### Review the Pandas-to-Spark Boundary

Pandas is reasonable here when individual API response batches are small enough to fit comfortably on the driver. If the volume increased significantly, I would minimize driver-side accumulation and move more normalization directly into Spark or persist raw responses in batches before distributed processing.

### Strengthen Idempotence and Data Quality

The Delta `MERGE` operations already provide useful upsert behavior for many datasets. I would extend that with explicit checks for:

- duplicate business keys
- missing IDs
- malformed timestamps
- null critical fields
- unexpected schema changes
- row-count anomalies

I would also review the member table's append behavior so repeat runs cannot unintentionally accumulate duplicate member snapshots unless history is intentionally being preserved.

### Fix Member-Activity Loop Control

The member activity collection currently uses `break` when a subscriber has no activity:

```python
if activity:
    ...
else:
    break
```

That can stop processing the remaining subscriber hashes. This should likely be `continue` so one subscriber with no activity does not terminate the entire loop.

### Safer Nested-Field Extraction

Some sections use defensive `.get()` or `try` / `except`, while others directly index nested Mailchimp fields. I would standardize nested-field handling so missing merge fields or optional report attributes cannot unexpectedly terminate a run.

### Reevaluate Table Optimization Strategy

The notebook runs `OPTIMIZE` and `ZORDER` directly after several merges. I would review actual table sizes, query patterns, and run frequency before optimizing every pipeline execution. Optimization should be driven by measured workload behavior rather than applied automatically to every table.

### Tune Enrichment Joins Based on Data Size

For the supplemental classification join, I would inspect the Spark physical plan with `.explain()` and consider broadcasting the CSV side when it is sufficiently small. I would also project only necessary columns before the join and avoid unnecessary persistence or shuffles.

### Add Observability

A production version would capture metrics such as:

- API records retrieved
- records inserted and updated
- records rejected or quarantined
- API failures and retries
- runtime per task
- latest successful extraction timestamp

This would make failures and data anomalies easier to detect before they affect downstream reporting.

## Summary

The notebook is an end-to-end Mailchimp ingestion and transformation workflow built in Databricks. It combines Python API collection with Pandas normalization, Spark processing, Delta Lake persistence, SQL `MERGE` operations, and supplemental enrichment. Its strongest patterns are the handling of multiple related API entities, business-key-aware upserts, and the transition from external API data into analytics-ready Delta datasets.

The main modernization opportunity is architectural: preserve raw responses first, separate collection from transformation, make extraction incremental, introduce stronger observability and quality controls, and orchestrate the components as discrete pipeline tasks.
