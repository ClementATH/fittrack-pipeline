# Glossary

## Data Engineering Terms Explained Simply

---

### Backoff (Exponential)
When a request fails, you wait before retrying — and each retry waits longer. First 5 seconds, then 10, then 20. Like knocking on a door: if nobody answers, you wait longer between knocks instead of banging faster.

### Batch ID
A unique identifier assigned to a group of records ingested together. Like a shipping tracking number — it tells you which delivery a package came in.

### Bronze Layer
The first layer in the Medallion Architecture. Stores raw data exactly as received from the source. Think of it as the "inbox" — everything lands here unmodified.

### Columnar Storage
A way of storing data where each column is stored separately (like Parquet, DuckDB, Snowflake). Traditional databases store data row by row. Columnar is faster for analytics because queries usually need only a few columns out of many.

### Context Manager
Python's `with` statement pattern. Guarantees cleanup happens even if errors occur. Like a try/finally, but cleaner: `with open(file) as f:` guarantees the file gets closed.

### CRUD
Create, Read, Update, Delete — the four basic database operations.

### DAG (Directed Acyclic Graph)
A way to define task dependencies. "Task B depends on Task A" means A runs first. Airflow and Prefect use DAGs to orchestrate pipelines. "Acyclic" means no circular dependencies.

### Data Lake
A storage system (usually S3, ADLS, or GCS) that holds raw data in files (Parquet, JSON, CSV). Unlike a database, there's no schema enforced at write time.

### Data Lakehouse
Combines the flexibility of a data lake with the structure of a data warehouse. Databricks Delta Lake and Snowflake both implement this pattern.

### Deduplication (Dedup)
Removing duplicate records. Critical when pipelines retry or sources send overlapping data.

### Dimension Table
A table describing the "who/what/where/when" of your business. In this project: `dim_exercises` (what), `dim_athletes` (who), `dim_date` (when). Usually small, with descriptive text columns.

### DuckDB
An in-process analytical database (like SQLite, but for analytics). Runs inside Python with no server needed. Great for development and testing.

### E1RM (Estimated One-Rep Max)
The maximum weight you could theoretically lift for one repetition, calculated from a multi-rep set. Formula: weight * (1 + reps/30). Used in this project for PR tracking.

### ETL (Extract, Transform, Load)
The classic data pipeline pattern: Extract data from sources, Transform it (clean, validate, enrich), Load it into a target system.

### Fact Table
A table storing measurable events — the "how much/how many" of your business. In this project: `fact_workouts` (volume, duration), `fact_nutrition` (calories, macros). Usually large, with numeric measure columns.

### Gold Layer
The third layer in the Medallion Architecture. Contains business-ready, modeled data (star schema). This is what dashboards and analysts query.

### Idempotent
An operation that produces the same result no matter how many times you run it. Like an elevator button — pressing it 5 times doesn't make it come 5 times faster.

### IQR (Interquartile Range)
A measure of statistical spread: Q3 - Q1 (the range of the middle 50% of data). Used for outlier detection because it's robust against extreme values.

### Medallion Architecture
A three-layer data organization: Bronze (raw), Silver (clean), Gold (business). Invented by Databricks. The industry standard for data lakehouses.

### Orchestrator
The component that coordinates pipeline execution — deciding what runs, in what order, and what to do when things fail. Airflow, Prefect, and Dagster are popular orchestrators.

### Parquet
A columnar file format that's compressed, typed, and fast. The standard file format for data engineering. Think of it as "Excel for big data" — but actually fast and reliable.

### Pydantic
A Python library for data validation using type annotations. Catches config errors at startup instead of mid-pipeline.

### Rate Limiting
Restricting how fast you call an API. If an API allows 60 requests/minute, you space your calls to stay under that limit.

### RLS (Row Level Security)
A database feature where different users see different rows in the same table. Each athlete only sees their own data.

### SCD (Slowly Changing Dimension)
A pattern for handling changes to dimension data over time:
- **Type 1:** Overwrite (no history)
- **Type 2:** Add new row with dates (full history) — used in this project
- **Type 3:** Add column for old value (limited history)

### Silver Layer
The second layer in the Medallion Architecture. Contains cleaned, validated, and transformed data. The "quality-assured" version of raw data.

### Snake Case
A naming convention where words are separated by underscores: `workout_date`, `body_fat_pct`. The Python and SQL standard.

### Star Schema
A dimensional modeling pattern with fact tables in the center and dimension tables around them, forming a star shape. Optimized for analytical queries.

### Surrogate Key
A warehouse-generated ID that's independent of the source system. If you switch APIs, your warehouse IDs stay stable.

### Z-Score
A measure of how many standard deviations a value is from the mean. z = (value - mean) / std_dev. Values with |z| > 3 are considered outliers.
