# System Architecture

## Overview

FitTrack Pro ETL Pipeline is a production-grade data pipeline following the **Medallion Architecture** (Bronze -> Silver -> Gold). It ingests fitness data from APIs and files, validates and transforms it, and loads it into a dimensional data warehouse.

## System Diagram

```
                        FitTrack Pro ETL Pipeline
    ============================================================

    DATA SOURCES                PIPELINE                    WAREHOUSE
    ============          ===================          ================

    +-------------+       +------------------+
    | Wger API    |------>| BRONZE LAYER     |       +----------------+
    | (exercises) |       | (Raw Ingestion)  |       | GOLD LAYER     |
    +-------------+       |                  |       | (Star Schema)  |
                          | - API pagination |       |                |
    +-------------+       | - Rate limiting  |       | dim_exercises  |
    | USDA API    |------>| - Retry logic    |       | dim_athletes   |
    | (nutrition) |       | - File watching  |       | dim_date       |
    +-------------+       | - Metadata tags  |       | dim_muscles    |
                          | - Parquet output |       |                |
    +-------------+       +--------+---------+       | fact_workouts  |
    | File Drops  |------>|        |                  | fact_metrics   |
    | (CSV/JSON)  |       |        v                  | fact_nutrition |
    +-------------+       +------------------+       +-------+--------+
                          | SILVER LAYER     |               |
                          | (Clean/Transform)|               |
                          |                  |       +-------v--------+
                          | - snake_case cols|       | DuckDB         |
                          | - Type casting   |       | (Analytical DB)|
                          | - Deduplication  |       +----------------+
                          | - Unit convert   |
                          | - Enrichment     |       +----------------+
                          +--------+---------+       | QUALITY ENGINE |
                                   |                 |                |
                                   v                 | - Profiling    |
                          +------------------+       | - Validation   |
                          | QUALITY GATE     |------>| - Anomaly Det. |
                          | Score >= 50?     |       | - Scoring 0-100|
                          +--------+---------+       | - Reports (MD) |
                                   |                 +----------------+
                                   v
                          +------------------+       +----------------+
                          | GOLD LAYER       |       | MONITORING     |
                          | (Dimensional)    |       |                |
                          |                  |       | - Health checks|
                          | - SCD Type 2     |       | - Alerting     |
                          | - Star schema    |       | - Scheduling   |
                          | - Fact tables    |       | - Dashboard    |
                          | - Dim tables     |       | (Streamlit)    |
                          +------------------+       +----------------+
```

## Technology Choices

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.10+ | Industry standard for data engineering |
| Database | DuckDB | Columnar analytics DB, no server needed, SQL-compatible with Snowflake |
| File Format | Parquet | Columnar, compressed, typed — industry standard for data lakes |
| Config | YAML | Human-readable, version-controllable |
| Validation | Pydantic | Type-safe config models, fail-fast validation |
| HTTP | httpx | Modern async-capable client with retry support |
| Dashboard | Streamlit | Rapid dashboard development, Python-native |
| Scheduling | APScheduler | Lightweight, cron-compatible, no infrastructure |
| Testing | pytest | Standard Python testing with fixtures and parametrize |
| Logging | Python logging + JSON | Structured logs for machine parsing |

## Data Flow Detail

### 1. Bronze Layer (Raw Ingestion)
- Data is stored **exactly as received** from the source
- Metadata columns added: `_ingested_at`, `_source_name`, `_batch_id`, `_source_hash`
- Files stored as Parquet with timestamp-based naming for immutability
- **Idempotent**: Re-running never duplicates data (content hash dedup)

### 2. Silver Layer (Clean + Transform + Enrich)
- **Cleaning**: Column standardization, type casting, null handling, dedup
- **Transformation**: Source-specific mappings (muscle IDs -> names, unit conversions)
- **Enrichment**: Derived columns (BMI, macro percentages, training phase)
- Quality gate: Data must score >= 50/100 to proceed to Gold

### 3. Gold Layer (Business-Ready)
- **Dimensional model** (star schema) with fact and dimension tables
- **SCD Type 2** on dimensions for full history tracking
- Loads into DuckDB for analytical queries
- Parquet files for downstream consumption

## Folder Structure

```
Claude Data Engineer Work-Pipeline/
+-- 01_schema.sql ... 07_etl_procedures.sql  (existing + new SQL)
+-- pyproject.toml                            (dependencies)
+-- config/
|   +-- pipeline_config.yaml                  (pipeline settings)
|   +-- sources.yaml                          (data source definitions)
|   +-- quality_rules.yaml                    (validation rules)
+-- src/
|   +-- orchestrator.py                       (main pipeline brain)
|   +-- ingestion/     (Bronze)               (API + file ingestors)
|   +-- transformation/ (Silver)              (cleaner, transformer, enricher)
|   +-- warehouse/     (Gold)                 (dim_builder, fact_builder, SCD)
|   +-- quality/                              (profiler, validator, scorer)
|   +-- monitor/                              (scheduler, health, dashboard)
|   +-- utils/                                (logger, config, DB connector)
+-- data/
|   +-- bronze/  silver/  gold/               (pipeline data layers)
|   +-- incoming/                             (file drop zone)
|   +-- sample/                               (test data)
+-- tests/                                    (pytest suite)
+-- logs/                                     (structured JSON logs)
+-- reports/                                  (quality reports)
+-- docs/                                     (this documentation)
```
