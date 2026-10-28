
# Maritime Small-File ELT Pipeline

**Databricks | PySpark | Delta Lake | Python | Data Engineering | Data Quality**

## Project Overview

This project develops a batch ELT pipeline that integrates public maritime vessel data from three sources with different formats, schemas, and levels of detail.

The goal is to build a Databricks lakehouse pipeline that ingests and standardizes vessel information, resolves overlapping identifiers, and produces a unified Gold vessel table with weekly updates.

The project focuses on practical data engineering challenges, including:

- Ingesting heterogeneous data formats (TXT, XML, CSV)
- Investigating source schemas and data quality before pipeline development
- Resolving vessel identities across independent datasets
- Designing Bronze, Silver, and Gold Delta tables
- Managing small files, duplicate records, and incremental updates
- Preserving source lineage and documenting transformation decisions

**Current phase:** Source investigation completed; schema design is next.

## Data Sources

Three publicly available maritime datasets provide complementary vessel information.

| Source                    | Format                        | Purpose                                                           |
| ------------------------- | ----------------------------- | ----------------------------------------------------------------- |
| USCG Merchant Vessels     | Fixed-width TXT               | Vessel registration, identification, and physical characteristics |
| USCG PSIX / CGMIX         | SOAP/XML                      | Vessel details, Coast Guard activity, and compliance information  |
| NOAA / MarineCadastre AIS | Compressed CSV (`.csv.zst`) | Vessel positions, speed, course, and movement observations        |

Each source presents different parsing requirements, field conventions, and record structures.

For example, NOAA AIS contains time-stamped vessel positions, while the USCG Merchant dataset provides registry-level information. Combining these sources requires reconciling different record grains and determining which identifiers can reliably link vessels.

## Planned Architecture

The pipeline follows the Databricks Medallion Architecture.

```text
             PUBLIC MARITIME DATA SOURCES
                         |
         +---------------+---------------+
         |               |               |
    USCG Merchant     USCG PSIX       NOAA AIS
     TXT Files        SOAP/XML       CSV Files
         |               |               |
         +---------------+---------------+
                         |
                    RAW LANDING
               Original source data
                         |
                         v
                  BRONZE LAYER
               Raw Delta tables
               Ingestion metadata
                         |
                         v
                  SILVER LAYER
               Schema normalization
               Data quality checks
               Identifier resolution
               Deduplication
                         |
                         v
                   GOLD LAYER
                 Vessel Master
              One row per vessel
                         |
                         v
                 WEEKLY REFRESH
              Incremental processing
               Delta MERGE / Upsert
```

The proposed Gold table will combine vessel identifiers, registry attributes, and the latest available movement information, with selected compliance attributes where supported by the source data.

The design also includes investigation of small-file handling and Delta Lake optimization.

## Project Progress

### Step 0A — Project Definition

**Status: Completed**

Established the project requirements and proposed implementation approach.

Key decisions:

- Use three maritime source families
- Develop a batch ELT pipeline rather than streaming ingestion
- Follow a Bronze/Silver/Gold architecture
- Target one canonical Gold vessel table
- Design a weekly incremental refresh process
- Begin with representative samples before scaling ingestion volume
- Preserve raw source data and lineage

### Step 0B — Source Investigation and Profiling

**Status: Initial investigation completed**

Developed Python exploration notebooks to retrieve, inspect, and profile each source before designing the unified schema.

| Investigation         | Work completed                                                                                                         |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| USCG Merchant Vessels | Retrieved and parsed fixed-width text records; investigated field positions, identifiers, and data types               |
| USCG PSIX / CGMIX     | Tested SOAP service access; parsed XML responses; explored vessel-level attributes and a cross-source match            |
| NOAA AIS              | Retrieved and inspected compressed CSV data; profiled timestamps, identifiers, missing values, and vessel observations |

**Initial findings:**

- Different sources represent different grains: vessel registry records, service responses, and time-stamped movement observations.
- IMO, MMSI, and USCG vessel identifiers may support cross-source matching, but cannot be treated as interchangeable.
- AIS records contain incomplete identifier information, making identity resolution an important design consideration.
- Source-specific parsing and validation are necessary before data can be standardized.

Exploration notebooks are maintained as part of the project to document the investigation process.

### Step 0C — Schema Design and Source-to-Target Mapping

**Status: Next phase**

Planned work:

- Build a comparative data dictionary for all three sources
- Map original fields to standardized column names and data types
- Identify shared, unique, and ambiguous attributes
- Define vessel identity-matching and source precedence rules
- Establish data-quality validation requirements
- Finalize the initial Bronze, Silver, and Gold schemas

### Upcoming Development

| Phase         | Planned work                                                        |
| ------------- | ------------------------------------------------------------------- |
| Data Landing  | Preserve original files and source metadata                         |
| Bronze        | Ingest raw source records into Delta tables                         |
| Silver        | Parse, standardize, validate, and reconcile vessel records          |
| Gold          | Create a unified vessel master table                                |
| Orchestration | Implement a repeatable weekly refresh                               |
| Optimization  | Evaluate small-file behavior and Delta Lake optimization techniques |
| Validation    | Test transformations, incremental updates, and data quality         |

## Engineering Focus

### Heterogeneous Data Integration

The project combines fixed-width text, XML web-service responses, and compressed CSV files in a common lakehouse architecture.

### Vessel Identity Resolution

A primary challenge is determining when records from separate sources refer to the same vessel, while avoiding incorrect matches and preserving original identifiers.

### Small-File Management

The pipeline will examine how file sizes and file counts affect Delta Lake processing, with opportunities to evaluate compaction and other optimization strategies.

### Data Quality and Lineage

The design emphasizes schema documentation, identifier validation, source traceability, and repeatable transformations.

### Incremental Processing

The planned weekly workflow will identify new or changed source data and update the Gold table without unnecessarily rebuilding the entire dataset.

## Technologies

| Technology   | Role                                                         |
| ------------ | ------------------------------------------------------------ |
| Python       | Source investigation, extraction, and parsing                |
| Pandas       | Exploratory profiling and schema inspection                  |
| Databricks   | Planned lakehouse development and orchestration              |
| PySpark      | Planned distributed transformations                          |
| Delta Lake   | Planned table storage, incremental updates, and optimization |
| SQL          | Planned data validation and analytical queries               |
| Git / GitHub | Version control and project documentation                    |

## Project Status

This is an active portfolio project. Source exploration is complete for an initial set of representative samples, and schema design is the next development phase.

The implementation will be expanded incrementally, with additional documentation and pipeline code added as each phase is completed.
