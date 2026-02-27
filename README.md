# FitTrack Pro -- Data Engineering Pipeline

## A production-grade ETL pipeline built on the Medallion Architecture

**Original**: Supabase learning database with 60+ exercises, 12 weeks of training data, and full SQL analytics.
**Extended**: Complete ETL pipeline system with Bronze/Silver/Gold layers, data quality engine, and monitoring dashboard.

---

## What This Project Is

FitTrack Pro started as a fitness database for learning Supabase SQL. This pipeline extension transforms it into a full data engineering learning platform that demonstrates real-world patterns used at companies like Snowflake, dbt, and Airflow -- all running locally with zero cloud infrastructure.

**If you're learning data engineering**, this project teaches you:
- How ETL pipelines actually work (not just theory)
- The Medallion Architecture (Bronze/Silver/Gold) used by Databricks and modern data teams
- Data quality scoring, validation, and anomaly detection
- Dimensional modeling (star schema, SCD Type 2)
- Pipeline monitoring, alerting, and health checks

---

## Quick Start (5 minutes)

### Prerequisites
- Python 3.10+ (`py -3 --version`)
- pip (comes with Python)

### Install and Run

```bash
# 1. Install dependencies
py -3 -m pip install pandas duckdb pyarrow pydantic pydantic-settings pyyaml httpx

# 2. Run the full pipeline demo
py -3 run_demo.py

# 3. (Optional) Launch the monitoring dashboard
py -3 -m pip install streamlit plotly
py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501
```

The demo will:
1. Run health checks on all system components
2. Build reference dimensions (date, muscle groups, athletes)
3. Ingest 4 sample files (body metrics, exercises, nutrition, workouts)
4. Clean, transform, and enrich each dataset independently
5. Run full quality assessment (profiling, validation, anomaly detection, scoring)
6. Load passing datasets into DuckDB (the Gold warehouse)
7. Run analytical queries to verify everything worked

### Run Tests

```bash
py -3 -m pip install pytest
py -3 -m pytest tests/ -v
# 110 tests, all passing
```

---

## Architecture: The Medallion Pattern

```
   DATA SOURCES                BRONZE              SILVER                GOLD
  +-------------+         +-----------+       +-------------+      +-----------+
  | Wger API    |-------->| Raw JSON/ |------>| Cleaned     |----->| DuckDB    |
  | USDA API    |         | Parquet   |       | Transformed |      | Warehouse |
  | CSV/JSON    |         | (as-is)   |       | Enriched    |      | (Star     |
  | File Drops  |         +-----------+       +-------------+      |  Schema)  |
  +-------------+              |                    |               +-----------+
                               v                    v                    |
                         data/bronze/         data/silver/               v
                                                                   Dimensions:
                      QUALITY ENGINE                               - dim_date
                    +-----------------+                             - dim_exercises
                    | Profile         |                             - dim_athletes
                    | Validate        |                             - dim_muscles
                    | Detect Anomaly  |                             Facts:
                    | Score (0-100)   |----> Quality Gate           - gold_workouts
                    | Report          |      (score >= 50           - gold_body_metrics
                    +-----------------+       to proceed)           - gold_nutrition_logs
```

### Layer Responsibilities

| Layer | Purpose | Storage | Example |
|-------|---------|---------|---------|
| **Bronze** | Raw data exactly as received | Parquet files | API responses, CSV uploads |
| **Silver** | Cleaned, transformed, enriched | Parquet files | Snake_case columns, unit conversions, derived fields |
| **Gold** | Business-ready analytical data | DuckDB tables | Star schema with dimensions and facts |

---

## Project Structure

```
fittrack-pipeline/
|-- run_demo.py                    # Full pipeline demonstration
|-- config/
|   |-- pipeline_config.yaml       # Central pipeline configuration
|   |-- quality_rules.yaml         # Data quality rules (YAML-driven)
|   |-- sources.yaml               # Data source configurations
|-- src/
|   |-- ingestion/                 # Bronze layer (data intake)
|   |   |-- base_ingestor.py       # Abstract base with metadata + hashing
|   |   |-- api_ingestor.py        # REST API with pagination + retry
|   |   |-- file_ingestor.py       # CSV/JSON file drop zone
|   |-- transformation/            # Silver layer (data processing)
|   |   |-- cleaner.py             # Generic cleaning (snake_case, dedup, trim)
|   |   |-- transformer.py         # Source-specific transforms (unit conversion, mapping)
|   |   |-- enricher.py            # Derived fields (BMI, macro ratios, recovery index)
|   |-- warehouse/                 # Gold layer (dimensional modeling)
|   |   |-- dim_builder.py         # Dimension tables (date, athletes, muscles, exercises)
|   |   |-- fact_builder.py        # Fact table assembly
|   |   |-- scd_handler.py         # SCD Type 2 history tracking
|   |-- quality/                   # Data quality engine
|   |   |-- profiler.py            # Statistical profiling
|   |   |-- validator.py           # Schema + business rule validation
|   |   |-- anomaly_detector.py    # Z-score and IQR anomaly detection
|   |   |-- scorer.py              # Quality scoring (0-100, 4 dimensions)
|   |   |-- reporter.py            # Markdown quality reports
|   |-- monitor/                   # Operational monitoring
|   |   |-- alerter.py             # JSON-based alerting
|   |   |-- health_check.py        # System health checks
|   |   |-- dashboard.py           # Streamlit monitoring dashboard
|   |-- utils/                     # Shared utilities
|   |   |-- config_loader.py       # Pydantic config models
|   |   |-- db_connector.py        # DuckDB connection manager
|   |   |-- logger.py              # Structured JSON logging
|   |-- orchestrator.py            # Pipeline orchestration (facade pattern)
|   |-- scheduler.py               # Cron-based scheduling
|-- data/
|   |-- sample/                    # Sample data files (copy to incoming/)
|   |-- incoming/                  # File drop zone (pipeline watches this)
|   |-- bronze/                    # Raw ingested data (Parquet)
|   |-- silver/                    # Cleaned/transformed data (Parquet)
|   |-- gold/                      # Dimension Parquet files
|   |-- fittrack.duckdb            # DuckDB warehouse (created on first run)
|-- sql/                           # Original Supabase SQL files
|   |-- 01_schema.sql              # Table definitions, types, indexes
|   |-- 02_seed_exercises.sql      # 60+ exercise library
|   |-- 03_seed_athlete_data.sql   # 12 weeks of procedural data
|   |-- 04_views_functions.sql     # Views, RPCs, full-text search
|   |-- 05_rls_policies.sql        # Row Level Security + Realtime
|   |-- 06_staging.sql             # Staging tables for ETL
|   |-- 07_etl_procedures.sql      # Stored procedures for pipeline
|-- tests/                         # Test suite (110 tests)
|   |-- conftest.py                # Shared fixtures
|   |-- test_ingestion.py          # Bronze layer tests (19)
|   |-- test_transformation.py     # Silver layer tests (27)
|   |-- test_quality.py            # Quality engine tests (27)
|   |-- test_pipeline_e2e.py       # End-to-end integration tests (12)
|-- docs/                          # Documentation (8 guides)
|   |-- ARCHITECTURE.md            # System architecture deep-dive
|   |-- PIPELINE_GUIDE.md          # How the pipeline works
|   |-- DATA_QUALITY_GUIDE.md      # Quality engine explained
|   |-- MONITORING_GUIDE.md        # Dashboard and alerting
|   |-- SETUP.md                   # Installation and configuration
|   |-- EXISTING_SYSTEM.md         # Original SQL codebase docs
|   |-- LESSONS_LEARNED.md         # Data engineering lessons
|   |-- GLOSSARY.md                # Term definitions
|-- reports/                       # Generated quality reports (Markdown)
|-- logs/                          # Pipeline logs (JSON structured)
```

---

## Data Quality Engine

Every dataset is scored on 4 dimensions before it can reach the Gold layer:

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| **Completeness** | 30% | Missing values, null percentages |
| **Accuracy** | 30% | Business rule violations, type mismatches |
| **Consistency** | 20% | Anomalies, statistical outliers |
| **Timeliness** | 20% | Data freshness vs. SLA |

**Quality Gate**: Datasets scoring below 50/100 are blocked from Gold. This prevents bad data from reaching analytics.

### Example Output (from demo)

```
Dataset              Rows   Score  Grade     Status Gold Table
Body Metrics            7    99.8     A+     LOADED gold_body_metrics
Exercises               5    97.1     A+     LOADED gold_exercises
Nutrition Logs         10    93.3      A     LOADED gold_nutrition_logs
Workout Logs           10    76.0      C     LOADED gold_workouts
```

---

## Data Sources

| Source | Type | What It Provides |
|--------|------|-----------------|
| **Wger Fitness API** | REST API | Exercise database with muscles, equipment, categories |
| **USDA FoodData Central** | REST API | Nutritional data for foods |
| **File Drop Zone** | CSV/JSON files | Workout logs, body metrics, nutrition logs |

The file drop zone watches `data/incoming/` for new files. Drop a CSV or JSON file there, and the pipeline will automatically ingest, process, and load it.

---

## Original SQL Database

The original FitTrack Pro Supabase database is preserved in the `sql/` directory. To use it:

1. Go to [supabase.com](https://supabase.com) and create a new project
2. Open **SQL Editor** in the left sidebar
3. Paste and run each file **in order**:
   - `01_schema.sql` -- Tables, types, indexes, constraints
   - `02_seed_exercises.sql` -- 60+ exercise library
   - `03_seed_athlete_data.sql` -- 12 weeks of training data
   - `04_views_functions.sql` -- Views, RPCs, analytics
   - `05_rls_policies.sql` -- Row Level Security + Realtime

The pipeline's DuckDB schema mirrors this Postgres schema, so you can develop locally and deploy to Supabase when ready.

---

## Key Concepts Demonstrated

| Concept | Where It's Used | Learn More |
|---------|----------------|------------|
| Medallion Architecture | Bronze/Silver/Gold layers | `docs/ARCHITECTURE.md` |
| Idempotent Ingestion | Content hashing in `base_ingestor.py` | `docs/PIPELINE_GUIDE.md` |
| SCD Type 2 | `scd_handler.py` history tracking | `docs/PIPELINE_GUIDE.md` |
| Data Quality Scoring | `scorer.py` (4-dimension scoring) | `docs/DATA_QUALITY_GUIDE.md` |
| Exponential Backoff | `api_ingestor.py` retry logic | `docs/PIPELINE_GUIDE.md` |
| Strategy Pattern | `BaseIngestor` -> `APIIngestor`/`FileIngestor` | `docs/ARCHITECTURE.md` |
| Context Managers | `DuckDBConnector.connection()` | `docs/LESSONS_LEARNED.md` |
| YAML-Driven Config | `config/quality_rules.yaml` | `docs/SETUP.md` |
| Star Schema | Dimension + Fact tables in Gold | `docs/ARCHITECTURE.md` |
| Anomaly Detection | Z-score and IQR in `anomaly_detector.py` | `docs/DATA_QUALITY_GUIDE.md` |

---

## Documentation

| Guide | What You'll Learn |
|-------|------------------|
| [Architecture](docs/ARCHITECTURE.md) | System design, layer responsibilities, data flow |
| [Pipeline Guide](docs/PIPELINE_GUIDE.md) | How Bronze/Silver/Gold processing works |
| [Data Quality Guide](docs/DATA_QUALITY_GUIDE.md) | Profiling, validation, scoring, anomaly detection |
| [Monitoring Guide](docs/MONITORING_GUIDE.md) | Dashboard, alerting, health checks |
| [Setup Guide](docs/SETUP.md) | Installation, configuration, dependencies |
| [Existing System](docs/EXISTING_SYSTEM.md) | Original SQL codebase documentation |
| [Lessons Learned](docs/LESSONS_LEARNED.md) | Data engineering patterns and practices |
| [Glossary](docs/GLOSSARY.md) | Data engineering terminology |
| [Roadmap](docs/ROADMAP.md) | Future enhancements and next steps |

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | Python 3.10+ | Industry standard for data engineering |
| Database | DuckDB | Local columnar DB (mirrors Snowflake patterns) |
| Storage | Apache Parquet | Columnar, compressed, typed file format |
| Config | YAML + Pydantic | Type-safe, human-readable configuration |
| HTTP | httpx | Async-capable HTTP client with retry |
| Dashboard | Streamlit + Plotly | Rapid prototyping for data apps |
| Testing | pytest | Standard Python test framework |
| Logging | Python logging (JSON) | Structured, machine-parseable logs |

---

Built for learning. Train hard, query harder.
