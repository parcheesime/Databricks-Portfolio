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