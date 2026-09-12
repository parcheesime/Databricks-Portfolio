# LA Parking Citations Geospatial Data Pipeline

This project demonstrates an end-to-end data engineering workflow in Databricks using LA parking citation data and Los Angeles City Council district boundaries.

## What it demonstrates

- API ingestion into a Bronze Delta table
- Schema inspection and data-quality checks
- Silver-layer transformations and type normalization
- Sensitive-field masking
- Time and geospatial validation
- GeoPandas / Shapely point-in-polygon matching
- Enrichment of parking citations with City Council district assignments
- Gold-layer Delta table creation
- SQL validation of the final analytical dataset

## Architecture

Bronze  
Raw parking citation data preserved close to the source.

Silver  
Cleaned and normalized data with typed fields, quality checks, and masked sensitive values.

Gold  
Analysis-ready parking citation records enriched with City Council district information.

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