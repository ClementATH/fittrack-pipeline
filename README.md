# FitTrack Pro -- Data Engineering Pipeline

## A production-grade ETL pipeline built on the Medallion Architecture

---

## What Is This Project?

FitTrack Pro is a **complete data engineering pipeline** that processes fitness and training data the same way companies like Spotify, Netflix, and Airbnb process theirs -- just applied to the gym instead of music or movies.

Think of it this way: every time an athlete logs a workout, tracks their weight, or records what they ate, that raw data needs to be **collected**, **cleaned up**, **organized**, and **stored** in a way that makes it easy to answer questions like:

- *"Is Marcus actually getting stronger, or just lifting the same weight every week?"*
- *"Is Priya eating enough protein for her training volume?"*
- *"Which athlete has the best recovery scores, and what are they doing differently?"*

This pipeline does all of that automatically. It takes messy, real-world data from multiple sources (APIs, CSV files, JSON files), runs it through a structured cleaning and validation process, and loads it into a warehouse where it's ready for analysis -- complete with a 12-page interactive dashboard to explore everything visually.

### Why Does This Matter?

Every company that works with data -- whether it's a tech startup, a hospital, a bank, or a fitness app -- needs some version of this pipeline. The tools and vendor names change, but the core pattern is always the same:

1. **Collect** raw data from wherever it lives
2. **Clean** it so it's consistent and trustworthy
3. **Enrich** it with calculated fields that add business value
4. **Validate** it so bad data never reaches your reports
5. **Store** it in a structured warehouse for fast querying
6. **Monitor** the whole process so you know when something breaks

FitTrack Pro demonstrates all six of these steps with real, working code -- not slides or diagrams.

---

## The Athletes

All data in this pipeline is generated from seven athletes, each with a distinct training philosophy, body composition, and nutritional approach. Their profiles drive 30 days of realistic, seeded data -- 2,600+ rows total -- that flows through the entire pipeline.

### Marcus Chen -- The Powerlifter
| | |
|---|---|
| **Age** | 28 (born March 15, 1997) |
| **Height / Weight** | 180.3 cm / ~82 kg |
| **Body Fat** | ~14.5% |
| **Resting HR** | 58 bpm |
| **Experience** | 6 years |
| **Schedule** | 4 days/week (Mon, Wed, Fri, Sat) |
| **Daily Calories** | 3,200 kcal (2.2g protein/kg) |

Marcus trains for raw strength. His programming follows a classic push/pull/legs/upper split, built around the big three: bench press (100 kg), squat (130 kg), and deadlift (160 kg). He's in a slow bulk phase, gaining roughly 0.03 kg/day. Saturdays are his lighter upper-body days -- incline press, lateral raises, curls -- to build muscle without burning out his central nervous system before Monday's heavy session.

### Priya Sharma -- The CrossFit Athlete
| | |
|---|---|
| **Age** | 25 (born August 22, 2000) |
| **Height / Weight** | 165.0 cm / ~62 kg |
| **Body Fat** | ~19.0% |
| **Resting HR** | 52 bpm |
| **Experience** | 4 years |
| **Schedule** | 5 days/week (Mon, Tue, Thu, Fri, Sat) |
| **Daily Calories** | 2,400 kcal (2.0g protein/kg) |

Priya is the most conditioned athlete in the dataset -- her resting heart rate of 52 bpm reflects serious cardiovascular fitness. She trains five days per week with a functional approach: front squats, pull-ups, overhead presses, deadlifts, and planks. No single session is too long or too specialized. Her weight is stable (zero trend), which means her calories perfectly match her output. She's the athlete you'd compare everyone else against for training consistency.

### James O'Brien -- The Bodybuilder
| | |
|---|---|
| **Age** | 31 (born November 30, 1994) |
| **Height / Weight** | 188.0 cm / ~95 kg |
| **Body Fat** | ~18.0% |
| **Resting HR** | 62 bpm |
| **Experience** | 8 years |
| **Schedule** | 5 days/week (Mon, Tue, Wed, Fri, Sat) |
| **Daily Calories** | 2,800 kcal (2.5g protein/kg) |

James is the biggest and most experienced athlete in the dataset. At 188 cm and 95 kg, he carries the most muscle mass and eats the most protein per kilogram of bodyweight (2.5 g/kg). His programming is a traditional bodybuilding split -- push day (bench, incline, cable fly, dips), pull day (rows, pull-ups, face pulls, curls), leg day (squats, leg press, leg curls, calf raises), shoulder day, and a second back day focused on deadlifts. He's in a slow cut, losing about 0.04 kg/day. His higher resting heart rate (62 bpm) is typical for athletes who prioritize size over cardio.

### Sofia Rodriguez -- The Strength Athlete
| | |
|---|---|
| **Age** | 26 (born April 10, 1999) |
| **Height / Weight** | 170.5 cm / ~68 kg |
| **Body Fat** | ~20.0% |
| **Resting HR** | 55 bpm |
| **Experience** | 5 years |
| **Schedule** | 4 days/week (Mon, Wed, Fri, Sat) |
| **Daily Calories** | 2,200 kcal (1.8g protein/kg) |

Sofia trains with a pure strength focus -- low reps, heavy compounds, minimal isolation work. Monday is squat day (barbell squat at 90 kg, front squat, leg press). Wednesday is upper body (bench and overhead press). Friday is dedicated to pulls (deadlift at 120 kg, Romanian deadlifts, rows). Saturday is a lighter active-recovery session with pull-ups, lateral raises, and planks. Her weight is perfectly stable, and her programming is the most disciplined in the dataset -- she picks the basics and gets better at them week after week.

### Tyler Washington -- The Hybrid Athlete
| | |
|---|---|
| **Age** | 29 (born July 18, 1996) |
| **Height / Weight** | 175.8 cm / ~75 kg |
| **Body Fat** | ~15.0% |
| **Resting HR** | 50 bpm |
| **Experience** | 3 years |
| **Schedule** | 3 days/week (Mon, Wed, Fri) |
| **Daily Calories** | 2,600 kcal (1.6g protein/kg) |

Tyler has the lowest resting heart rate in the dataset (50 bpm), suggesting he does significant conditioning work outside the weight room. He only lifts three days per week -- full-body sessions each time -- which is the most efficient approach in the group. Monday: bench and rows. Wednesday: squats and deadlifts. Friday: overhead press, pull-ups, and tricep pushdowns. He's in a very slow lean bulk (0.015 kg/day). With only 3 years of experience, his numbers are modest but climbing fast. He represents the athlete who balances lifting with running, sports, or other activities.

### Aiko Tanaka -- The Calisthenics Specialist
| | |
|---|---|
| **Age** | 25 (born January 5, 2001) |
| **Height / Weight** | 160.0 cm / ~58 kg |
| **Body Fat** | ~17.0% |
| **Resting HR** | 54 bpm |
| **Experience** | 3 years |
| **Schedule** | 4 days/week (Mon, Tue, Thu, Fri) |
| **Daily Calories** | 2,000 kcal (1.8g protein/kg) |

Aiko is the lightest athlete in the dataset and the only one whose training is primarily bodyweight-based. Pull-ups and dips are her staple movements -- no external load, just body control. She supplements with light barbell squats (45 kg), overhead presses (25 kg), and planks for core stability. Her weight is stable, and her calorie target is the lowest in the group, which makes sense given her smaller frame. In the data, you'll see her exercises often show 0 kg for weight -- that's intentional, as bodyweight movements don't use external load.

### Clement Ohenhen Jr -- The Athletic Builder
| | |
|---|---|
| **Age** | 22 (born June 12, 2003) |
| **Height / Weight** | 182.9 cm (6'0") / ~78 kg |
| **Body Fat** | ~11.0% |
| **Resting HR** | 56 bpm |
| **Experience** | 4 years |
| **Schedule** | 5 days/week (Mon, Tue, Thu, Fri, Sat) |
| **Daily Calories** | 2,900 kcal (2.2g protein/kg) |

Clement is the leanest athlete in the dataset at 11% body fat -- visible six-pack with oblique striations and a shredded, athletic build. His programming follows a push/pull/legs structure during the week with a full-body athletic session on Saturdays (front squats, pull-ups, bench press, and planks). He's in a very slow lean bulk at 0.01 kg/day, maintaining his conditioning while gradually adding strength. His lifts are well-rounded: bench pressing 90 kg, squatting 120 kg, and deadlifting 140 kg, all with solid form. He eats 2,900 calories with 2.2g of protein per kilogram -- fueling growth without sacrificing his leanness. The combination of high training frequency (5 days) and low body fat makes his data particularly interesting for recovery score analysis.

---

## How the Pipeline Works (Plain English)

Imagine you're running a gym and seven athletes hand you their training logs every day. Some send you neatly typed spreadsheets. Others scribble on napkins. One of them uses kilograms, another uses pounds. Someone misspells "breakfast" as "brekfast."

This pipeline handles all of that. Here's the step-by-step:

### Step 1: Collect (Bronze Layer)

Raw data arrives from three possible sources:
- **Fitness APIs** (Wger for exercises, USDA for food nutrition)
- **CSV files** (workout logs, body measurements)
- **JSON files** (nutrition logs, exercise libraries)

The pipeline reads these files exactly as they are -- typos, inconsistencies, and all -- and saves them as Parquet files (a compressed, fast file format used in data engineering). Nothing is modified. This is the "single source of truth" you can always go back to.

### Step 2: Clean and Enrich (Silver Layer)

Now the pipeline processes each dataset:
- **Standardizes column names** (camelCase, ALL CAPS, "Weight (kg)" all become `weight_kg`)
- **Converts units** (pounds to kilograms, inconsistent date formats to ISO dates)
- **Removes duplicates** and trims whitespace
- **Calculates derived fields** like:
  - **Estimated 1-Rep Max** using the Epley formula: `weight x (1 + reps/30)`. This lets you compare a set of 100 kg for 8 reps against 120 kg for 3 reps on the same scale.
  - **Set Volume**: `weight x reps` per set -- the fundamental measure of training workload
  - **Intensity Zone**: based on RPE (Rate of Perceived Exertion), each set is classified as warm-up, working, hard, or max effort
  - **Body composition metrics**: fat mass, lean mass, BMI, weight change trends
  - **Macro splits**: what percentage of calories come from protein vs. carbs vs. fats

### Step 3: Validate (Quality Engine)

Before any data reaches the warehouse, it has to pass a quality check. Every dataset is scored from 0 to 100 across four dimensions:

| Dimension | What It Checks |
|-----------|---------------|
| **Completeness** (30%) | Are there missing values? Are all required columns present? |
| **Accuracy** (30%) | Do the numbers make sense? Is a body weight between 30-300 kg? Are calories non-negative? |
| **Consistency** (20%) | Are there statistical outliers? Does anything look abnormally high or low? |
| **Timeliness** (20%) | Is the data fresh enough? A workout log from 6 months ago might be stale. |

Datasets scoring below **50/100** are blocked from the warehouse entirely. This prevents bad data from contaminating your analytics -- a concept called a **quality gate**.

### Step 4: Store (Gold Layer)

Data that passes the quality gate is loaded into a **DuckDB warehouse** organized as a **star schema** -- the same design pattern used by Amazon, Walmart, and virtually every enterprise data warehouse:

- **Dimension tables** describe the "who, what, when": athletes, exercises, dates, muscle groups
- **Fact tables** store the "how much": every workout set, body measurement, and meal logged

This structure makes analytical queries fast and intuitive. Want to know Tyler's total squat volume for the month? That's a simple join between the workouts fact table and the athletes and exercises dimensions.

### Step 5: Monitor (Dashboard)

A 12-page Streamlit dashboard provides full visibility into both the data and the pipeline itself:

| Page | What It Shows |
|------|--------------|
| **Overview** | Pipeline health, layer counts, training summary KPIs |
| **Athlete Profiles** | Individual athlete cards with stats, trends, and PR tracking |
| **Strength Analytics** | Estimated 1RM trends, weekly volume charts, muscle group breakdown |
| **Nutrition Analytics** | Calorie trends, macro donut charts, meal distribution |
| **Body Composition** | Weight trends, body fat tracking, recovery score analysis |
| **Training Insights** | Weekly frequency, workout streaks, muscle balance radar |
| **Athlete Comparison** | Side-by-side KPIs and overlay charts for any two athletes |
| **Pipeline Runs** | Execution history, row counts, duration, status |
| **Data Quality** | Score breakdowns, validation results, anomaly counts |
| **Quality Trends** | Score evolution over time across pipeline runs |
| **Health Checks** | Disk space, database connectivity, directory integrity |
| **Alerts** | Triggered alerts, severity levels, resolution tracking |

---

## Quick Start

### Prerequisites
- Python 3.10+ (`py -3 --version`)
- pip (comes with Python)

### Install and Run

```bash
# 1. Install dependencies
py -3 -m pip install pandas duckdb pyarrow pydantic pydantic-settings pyyaml httpx rich

# 2. Run the full pipeline demo
py -3 run_demo.py

# 3. Launch the monitoring dashboard
py -3 -m pip install streamlit plotly
py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501
```

The demo processes **2,600+ rows** across 4 datasets in about 3 seconds:

```
Dataset              Rows   Score  Grade     Status Gold Table
Body Metrics          180    94.9      A     LOADED gold_body_metrics
Exercises              20    97.1     A+     LOADED gold_exercises
Nutrition Logs        932    90.6      A     LOADED gold_nutrition_logs
Workout Logs         1136    70.6      C     LOADED gold_workouts
```

### Run Tests

```bash
py -3 -m pip install pytest
py -3 -m pytest tests/ -v
# 186 tests, all passing
```

### Docker

```bash
docker compose build
docker compose --profile pipeline run --rm pipeline   # Run pipeline
docker compose up dashboard                           # Start dashboard
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

| Layer | Purpose | Storage | In Plain English |
|-------|---------|---------|-----------------|
| **Bronze** | Raw data exactly as received | Parquet files | "The filing cabinet -- everything goes in, nothing gets changed" |
| **Silver** | Cleaned, transformed, enriched | Parquet files | "The workshop -- raw material gets shaped into something useful" |
| **Gold** | Business-ready analytical data | DuckDB tables | "The showroom -- polished, organized, ready for questions" |

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
|   |   |-- enricher.py            # Derived fields (e1RM, volume, macros, recovery)
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
|   |   |-- contracts/             # Pydantic data contracts for Silver schemas
|   |-- monitor/                   # Operational monitoring
|   |   |-- alerter.py             # JSON-based alerting
|   |   |-- health_check.py        # System health checks
|   |   |-- dashboard.py           # Streamlit monitoring dashboard (12 pages)
|   |-- utils/                     # Shared utilities
|   |   |-- config_loader.py       # Pydantic config models
|   |   |-- db_connector.py        # DuckDB connection manager
|   |   |-- logger.py              # Structured JSON logging
|   |   |-- data_generator.py      # Synthetic data generator (seeded, reproducible)
|   |-- orchestrator.py            # Pipeline orchestration (facade pattern)
|   |-- scheduler.py               # Cron-based scheduling
|-- data/
|   |-- sample/                    # Generated sample data (2,600+ rows)
|   |-- incoming/                  # File drop zone (pipeline watches this)
|   |-- bronze/                    # Raw ingested data (Parquet)
|   |-- silver/                    # Cleaned/transformed data (Parquet)
|   |-- gold/                      # Dimension Parquet files
|   |-- fittrack.duckdb            # DuckDB warehouse (created on first run)
|-- tests/                         # Test suite (186 tests)
|   |-- conftest.py                # Shared fixtures
|   |-- test_ingestion.py          # Bronze layer tests
|   |-- test_transformation.py     # Silver layer tests
|   |-- test_quality.py            # Quality engine tests
|   |-- test_pipeline_e2e.py       # End-to-end integration tests
|   |-- test_contracts.py          # Data contract tests
|   |-- test_api_integration.py    # API integration tests
|   |-- test_data_generator.py     # Synthetic data generator tests
|-- docs/                          # Documentation (9 guides)
|-- reports/                       # Generated quality reports (Markdown)
|-- logs/                          # Pipeline logs (JSON structured)
```

---

## Data Sources

| Source | Type | What It Provides |
|--------|------|-----------------|
| **Wger Fitness API** | REST API | Exercise database with muscles, equipment, categories |
| **USDA FoodData Central** | REST API | Nutritional data for foods |
| **File Drop Zone** | CSV/JSON files | Workout logs, body metrics, nutrition logs |
| **Synthetic Generator** | Python script | 2,600+ rows of reproducible multi-athlete data |

The file drop zone watches `data/incoming/` for new files. Drop a CSV or JSON file there, and the pipeline will automatically ingest, process, and load it.

The synthetic data generator (`src/utils/data_generator.py`) creates deterministic datasets using a seeded random number generator -- run it twice with the same seed and you get byte-identical output. This makes testing reliable and demos reproducible.

---

## Key Concepts Demonstrated

| Concept | Where It's Used | What It Means |
|---------|----------------|---------------|
| Medallion Architecture | Bronze/Silver/Gold layers | Industry-standard pattern for organizing data by quality level |
| Star Schema | Gold layer dimensional model | Warehouse design that separates "what happened" (facts) from "who/what/when" (dimensions) |
| SCD Type 2 | `scd_handler.py` | Tracks how dimension records change over time (e.g., an athlete changing weight class) |
| Data Quality Scoring | `scorer.py` | Assigns a 0-100 score across completeness, accuracy, consistency, and timeliness |
| Quality Gate | Pipeline orchestrator | Blocks bad data from reaching the warehouse -- prevents garbage-in, garbage-out |
| Idempotent Ingestion | Content hashing in `base_ingestor.py` | Processing the same file twice doesn't create duplicates |
| Exponential Backoff | `api_ingestor.py` retry logic | When an API fails, wait progressively longer before retrying |
| Data Contracts | Pydantic models in `quality/contracts/` | Schema enforcement that catches structural errors before they become data bugs |
| Strategy Pattern | `BaseIngestor` -> `APIIngestor`/`FileIngestor` | Swappable data source handlers behind a common interface |
| Facade Pattern | `orchestrator.py` | One entry point coordinates all pipeline components |
| YAML-Driven Config | `config/quality_rules.yaml` | Business rules defined in config files, not hardcoded in Python |

---

## Tech Stack

| Component | Technology | Why This Choice |
|-----------|-----------|----------------|
| Language | Python 3.10+ | Industry standard for data engineering, rich ecosystem |
| Database | DuckDB | Embedded columnar database -- runs locally, mirrors Snowflake/BigQuery patterns |
| Storage | Apache Parquet | Columnar, compressed, strongly typed -- the standard for analytical data |
| Validation | Pydantic | Type-safe data contracts with automatic validation |
| Config | YAML + Pydantic | Human-readable config files with runtime type checking |
| HTTP | httpx | Modern HTTP client with async support and built-in retry |
| Dashboard | Streamlit + Plotly | Interactive data apps without frontend code |
| Testing | pytest | 186 tests covering ingestion, transformation, quality, e2e, and data generation |
| Linting | Ruff + mypy | Fast linting and static type analysis |
| Containers | Docker Compose | Reproducible builds with pipeline and dashboard profiles |
| CI/CD | GitHub Actions | Automated lint, type check, and test on every push |

---

## Documentation

| Guide | What You'll Learn |
|-------|------------------|
| [Architecture](docs/ARCHITECTURE.md) | System design, layer responsibilities, data flow |
| [Pipeline Guide](docs/PIPELINE_GUIDE.md) | How Bronze/Silver/Gold processing works step by step |
| [Data Quality Guide](docs/DATA_QUALITY_GUIDE.md) | Profiling, validation, scoring, anomaly detection |
| [Monitoring Guide](docs/MONITORING_GUIDE.md) | Dashboard pages, alerting rules, health checks |
| [Setup Guide](docs/SETUP.md) | Installation, configuration, dependencies |
| [Existing System](docs/EXISTING_SYSTEM.md) | Original Supabase SQL codebase documentation |
| [Lessons Learned](docs/LESSONS_LEARNED.md) | Data engineering patterns and practices |
| [Glossary](docs/GLOSSARY.md) | Data engineering terminology explained |
| [Roadmap](docs/ROADMAP.md) | Future enhancements and next steps |

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

Built for learning. Train hard, query harder.
