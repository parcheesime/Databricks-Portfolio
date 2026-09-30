# LA Parking Citations Geospatial Data Pipeline

This project demonstrates an end-to-end data engineering workflow in Databricks using Los Angeles parking citation data and Los Angeles City Council district boundaries.

The pipeline is implemented as a scheduled three-task Databricks Lakeflow Job that ingests, validates, transforms, enriches, and publishes citation data through Bronze, Silver, and Gold Delta tables. A separate visualization notebook consumes the Gold layer for downstream analysis.

## What it demonstrates

- REST API ingestion from the LA Open Data Socrata API
- Explicit source schema and source-aligned Bronze ingestion
- PySpark transformations and Delta Lake persistence
- Silver-layer type normalization and validation
- Data-quality enforcement with quarantine handling
- Sensitive-field masking
- Time and geographic-coordinate validation
- GeoPandas point-in-polygon enrichment
- Los Angeles City Council district assignment
- Gold-layer analytical dataset creation
- Three-task Databricks Lakeflow Job orchestration
- Task dependencies and scheduled execution
- Databricks Declarative Automation Bundle configuration in YAML
- Downstream geospatial visualization from the Gold layer

## Architecture

```text
LA Parking Citations API
          |
          v
  01_ingest_bronze
          |
          v
       Bronze
          |
          v
 02_transform_silver
          |
     Data Quality
       /       \
      /         \
   FAIL         PASS
    |             |
    v             v
Quarantine      Silver
                  |
                  v
          03_enrich_gold
                  |
        LA City Council
        ArcGIS Boundaries
                  |
                  v
                 Gold
                  |
                  v
       04_visualize_districts
        (downstream analysis)
```

## Lakeflow Job

The production-style pipeline is orchestrated as three sequential notebook tasks:

```text
01_ingest_bronze
        |
        v
02_transform_silver
        |
        v
03_enrich_gold
```

The job has been tested successfully end-to-end using Databricks Serverless compute.

The current development schedule runs weekly on Sunday at 6:00 AM in the `America/Los_Angeles` timezone.

`04_visualize_districts` is intentionally not part of the Lakeflow Job. It is a downstream analysis notebook that reads the completed Gold table.

## Data Quality

The Silver layer validates critical fields before records are allowed to proceed to Gold.

Critical checks include:

- Ticket number present
- Issue date successfully parsed
- Issue time within valid hour/minute ranges
- Coordinates present and within the expected Los Angeles-area range

Additional quality flags track fields such as fine amount and violation code without automatically rejecting otherwise usable records.

In the current 50,000-record portfolio batch:

- 50,000 records land in Bronze
- 49,954 records pass the Silver quality gate
- 46 records are quarantined for invalid coordinates
- 1,061 records have an invalid or missing violation code; this remains a non-blocking quality flag

Quarantined records are preserved for investigation rather than silently discarded.

## Geospatial Enrichment

The Gold task retrieves Los Angeles City Council district polygons from the LA ArcGIS REST API using EPSG:4326.

Valid Silver citation coordinates are converted to GeoPandas point geometry and joined to district polygons using a point-in-polygon spatial join:

```python
gold_gdf = gpd.sjoin(
    parking_gdf,
    districts_gdf[["District", "District_Name", "geometry"]],
    how="left",
    predicate="within"
)
```

The left join preserves citation records even if a point does not match a district boundary.

The current Gold dataset contains citations across all 15 Los Angeles City Council districts.

## Visualization

`04_visualize_districts` provides downstream exploratory analysis of the Gold table, including:

- Citation counts by council district
- Council district boundary maps
- Citation points colored by assigned district
- District number labels

Keeping visualization separate from the three-task Lakeflow Job demonstrates the distinction between pipeline processing and downstream analytics.

## Databricks Declarative Automation Bundle

The repository includes bundle configuration so the Lakeflow Job can be represented as code.

```text
databricks.yml
resources/
  la_parking_job.yml
```

- `databricks.yml` defines the Databricks bundle and development target.
- `resources/la_parking_job.yml` defines the Lakeflow Job, task dependencies, schedule, and notebook resources.

This mirrors the successfully tested job configuration in Databricks.

## Data Sources

- [Los Angeles Parking Citations](https://data.lacity.org/Transportation/Parking-Citations/wjz9-h9np) — City of Los Angeles parking citation dataset accessed through the Socrata API.
- [Los Angeles City Council District Boundaries](https://maps.lacity.org/lahub/rest/services) — ArcGIS boundary data used for point-in-polygon enrichment.

## Tech

- Databricks
- Databricks Lakeflow Jobs
- Databricks Declarative Automation Bundles
- PySpark
- Spark SQL
- Delta Lake
- Python
- Pandas
- GeoPandas
- Matplotlib

## Project Files

```text
01_ingest_bronze
02_transform_silver
03_enrich_gold
04_visualize_districts
databricks.yml
resources/
  la_parking_job.yml
```

The first three notebooks form the scheduled Lakeflow pipeline. The fourth notebook is a downstream Gold-layer visualization and analysis notebook.

## Scaling Considerations

This portfolio project intentionally uses a bounded API batch and GeoPandas for an understandable end-to-end implementation.

For larger production volumes, next steps would include:

- Paginated or incremental API ingestion
- Idempotent Bronze ingestion using Delta `MERGE`
- Distributed geospatial processing instead of `toPandas()`
- Configurable data-quality rules and metrics
- Environment-managed Python dependencies rather than notebook-scoped package installati

# LA Parking Citations Geospatial Data Pipeline

This project demonstrates an end-to-end data engineering workflow in Databricks using LA parking citation data and Los Angeles City Council district boundaries.

The pipeline is implemented as a scheduled Databricks job that ingests, validates, transforms, enriches, and publishes citation data through Bronze, Silver, and Gold Delta tables.

## What it demonstrates

- REST API ingestion into a Bronze Delta table
- PySpark and SQL-based transformations
- Schema inspection and data-quality profiling
- Silver-layer type normalization and validation
- Data-quality enforcement with quarantine handling
- Sensitive-field masking
- Time and geospatial validation
- GeoPandas / Shapely point-in-polygon matching
- Enrichment with Los Angeles City Council district assignments
- Gold-layer analytical dataset creation
- Scheduled Databricks job execution
- Job monitoring and failure email notifications
- SQL validation of pipeline outputs

## Architecture

```text
LA Parking Citations API
          |
          v
       Bronze
          |
          v
 Transform + Validate
          |
     Data Quality
       /       \
      /         \
   FAIL         PASS
    |             |
    v             v
Quarantine      Silver
                  |
                  v
          Geospatial Enrichment
                  |
                  v
                 Gold


## Data Sources

- [Los Angeles Parking Citations](https://data.lacity.org/Transportation/Parking-Citations/wjz9-h9np) — City of Los Angeles open-data parking citation dataset accessed through the Socrata API.
- [Los Angeles City Council District Boundaries](https://maps.lacity.org/lahub/rest/services) — Geographic boundary data used for point-in-polygon enrichment of citation locations.

## Tech

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Pandas
- GeoPandas
- Shapely

## Notebook

`LA Parking Citations - Geospatial Data Pipeline.ipynb`

The notebook includes executed outputs, schema validation, data-quality checks, geospatial validation, and the final Gold table.
```
