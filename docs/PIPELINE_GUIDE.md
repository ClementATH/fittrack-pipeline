# Pipeline Guide

## How the Pipeline Works, Step by Step

This guide walks through exactly what happens when you run `py -3 -m src.orchestrator`.

---

## Step 1: Initialization

The orchestrator loads three config files:
- `config/pipeline_config.yaml` — Pipeline settings
- `config/sources.yaml` — Data source definitions
- `config/quality_rules.yaml` — Validation rules

It then initializes all components: database connector, cleaner, transformer, enricher, quality engine, and alerter.

## Step 2: Health Checks

Before processing any data, the pipeline runs health checks:
- Disk space available
- Database accessible
- Data directories exist
- Memory sufficient
- Log directory writable

If any check fails, an alert is logged but the pipeline continues.

## Step 3: Reference Dimensions

The pipeline builds "reference" dimension tables that don't depend on source data:
- **dim_date** — Pre-computed date attributes (2025-2026)
- **dim_muscle_groups** — All 19 muscle groups with body regions
- **dim_athletes** — Athlete profiles (from seed data)

## Step 4: Source Processing (for each data source)

### 4a. Bronze Layer — Raw Ingestion

For API sources (Wger, USDA):
1. Build the request URL from config
2. Paginate through all pages of results
3. Handle rate limiting (sleep between requests)
4. Retry on failures with exponential backoff
5. Flatten nested JSON into a flat DataFrame
6. Add metadata columns (`_ingested_at`, `_source_name`, `_batch_id`, `_source_hash`)
7. Save as Parquet in `data/bronze/{source}/{dataset}_{timestamp}.parquet`

For file sources:
1. Scan `data/incoming/` for matching files
2. Read each file (CSV or JSON)
3. Validate file size and format
4. Add metadata and source file name
5. Save to Bronze, move original to `data/incoming/processed/`

### 4b. Silver Layer — Clean + Transform + Enrich

**Cleaning** (generic operations):
1. Standardize column names to `snake_case`
2. Strip whitespace from all strings
3. Handle null values
4. Cast data types
5. Remove duplicate rows

**Transformation** (source-specific logic):
- Wger: Map muscle IDs to names, equipment IDs to names, generate slugs
- USDA: Extract nutrients from nested arrays, rename columns
- Files: Convert pounds to kg, inches to cm, parse date strings

**Enrichment** (derived columns):
- Exercises: compound_score, movement_category
- Workouts: day_of_week, week_number, volume_per_set
- Body metrics: fat_mass_kg, weight_change, recovery_index
- Nutrition: macro percentages, calorie contributions per macro

Result saved as Parquet in `data/silver/{source}/{dataset}.parquet`

### 4c. Quality Gate

Every dataset gets a full quality assessment:
1. **Profile**: Column statistics, null counts, distributions
2. **Validate**: Schema rules, business rules, freshness checks
3. **Anomaly Detection**: Z-score and IQR outlier detection
4. **Score**: 0-100 score across 4 dimensions (Completeness, Accuracy, Consistency, Timeliness)
5. **Report**: Markdown report saved to `reports/`

**If quality score < 50**, the data is BLOCKED from entering Gold. An alert is raised.

### 4d. Gold Layer — Load to Warehouse

Data is loaded into the star schema:
- **Dimension tables**: SCD Type 2 applied (tracks historical changes)
- **Fact tables**: Loaded with foreign keys to dimensions
- **DuckDB**: Tables created/updated for analytical queries
- **Parquet**: Gold files saved for external consumption

## Step 5: Run Logging

The pipeline saves:
- Run metadata to `pipeline_runs` table in DuckDB
- Full JSON run log to `logs/pipeline_run_{timestamp}.json`
- Quality scores to `quality_scores` table
- Alerts to `logs/alerts.json`

## Step 6: Summary

The pipeline prints a summary:
```
============================================================
PIPELINE RUN SUMMARY
============================================================
  Run ID:    a1b2c3d4
  Duration:  45.2s
  Rows:      1,234
  Errors:    0
  [OK] wger_exercises: 890 rows
  [OK] file_drop_zone: 344 rows
============================================================
```

---

## Data Flow Diagram

```
Source -> Bronze (raw parquet) -> Silver (clean parquet) -> Quality Gate -> Gold (star schema)
                                                              |
                                                              v
                                                         Quality Report
                                                         (Markdown)
```
