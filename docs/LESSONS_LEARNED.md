# Lessons Learned

## Key Data Engineering Concepts Demonstrated in This Project

This document maps every major pattern in this project to real-world concepts you'll encounter at WellMed, in interviews, and building Forto Research's data infrastructure.

---

## 1. Medallion Architecture (Bronze -> Silver -> Gold)

**What:** A three-layer data organization pattern where each layer has a specific purpose.

**Where it's used:** Databricks (invented the term), Delta Lake, every modern data lakehouse.

**In this project:**
- Bronze = `data/bronze/` — Raw API responses and file uploads, stored exactly as received
- Silver = `data/silver/` — Cleaned, typed, deduplicated data
- Gold = `data/gold/` — Star schema with facts and dimensions

**Interview tip:** When asked "how do you organize your data lake?" — describe this pattern. It's the expected answer.

---

## 2. Idempotency

**What:** Running the same operation multiple times produces the same result.

**Why it matters:** Pipelines fail and get retried. If re-running creates duplicates, your data is wrong.

**In this project:**
- Content hashing (`_source_hash`) detects if source data changed
- Deduplication in the Silver layer removes duplicates from re-runs
- SCD Type 2 uses business keys to detect new vs. existing records

**At WellMed:** If your ETL job fails halfway and restarts, patients shouldn't appear twice.

---

## 3. Slowly Changing Dimensions (SCD Type 2)

**What:** Tracks historical changes to dimension records by keeping both old and new versions with effective dates.

**In this project:** `src/warehouse/scd_handler.py` — When an exercise's difficulty changes from "intermediate" to "advanced", both versions are kept with date ranges.

**At WellMed:** Patient address changes are SCD Type 2. You need to know which address was active for each claim.

**Interview tip:** This is one of the most commonly asked dimensional modeling questions. Explain Types 1, 2, and 3.

---

## 4. Star Schema (Dimensional Modeling)

**What:** Fact tables (events/measurements) surrounded by dimension tables (context/descriptors).

**In this project:**
- Facts: `fact_workouts`, `fact_body_metrics`, `fact_nutrition`
- Dimensions: `dim_exercises`, `dim_athletes`, `dim_date`, `dim_muscle_groups`

**Why not just one big table?** Star schemas are optimized for analytical queries. JOINs are fast because dimensions are small. Aggregations on facts are efficient because measures are numeric.

---

## 5. Data Quality as a First-Class Citizen

**What:** Treating data quality with the same rigor as code quality.

**In this project:** A full quality framework with profiling, validation, anomaly detection, scoring, and reporting — all YAML-configurable.

**Key insight:** Quality gates (score < 50 = blocked) prevent bad data from reaching dashboards. This is better than finding errors after a stakeholder complains.

---

## 6. Configuration-Driven Architecture

**What:** Business logic and rules defined in config files, not hardcoded.

**In this project:**
- `sources.yaml` — Adding a new API source is a config change
- `quality_rules.yaml` — Adding a validation rule is a config change
- `pipeline_config.yaml` — Changing retry counts, batch sizes, paths

**Why:** Domain experts (coaches, nutritionists) can update rules without writing Python.

---

## 7. Structured Logging

**What:** JSON-formatted logs instead of plain text `print()` statements.

**In this project:** Every log line is JSON with timestamp, level, module, source, and layer fields.

**Why:** Log aggregation tools (Splunk, Datadog, CloudWatch) can parse JSON automatically. Try that with `print("something broke")`.

---

## 8. Retry with Exponential Backoff

**What:** When a request fails, wait progressively longer before retrying: 5s, 10s, 20s.

**In this project:** `src/ingestion/api_ingestor.py` — API calls retry 3 times with exponential backoff.

**Why not just retry immediately?** If the server is overloaded, hammering it with retries makes it worse. Backoff gives it time to recover.

---

## 9. Context Manager Pattern

**What:** Using `with` statements to guarantee resource cleanup.

**In this project:** `DuckDBConnector.connection()` ensures database connections are always closed, even if an error occurs.

**Python fundamental:** Every file open, database connection, and network socket should use a context manager.

---

## 10. Abstract Base Classes and Strategy Pattern

**What:** Defining a common interface that multiple implementations follow.

**In this project:** `BaseIngestor` defines the contract. `APIIngestor` and `FileIngestor` implement it differently. The orchestrator doesn't care which one — it calls `ingestor.ingest()`.

**Why:** Adding a new source type (Kafka, database, SFTP) means writing one new class, not changing existing code.

---

## What to Study Next

1. **Run the pipeline end-to-end** and read every log line
2. **Add a new validation rule** in `quality_rules.yaml` and see it appear in reports
3. **Drop a CSV file** in `data/incoming/` and trace it through all three layers
4. **Modify the SCD handler** to implement SCD Type 1 (overwrite) and see the difference
5. **Break something on purpose** (invalid data, missing columns) and see how the quality gate catches it
