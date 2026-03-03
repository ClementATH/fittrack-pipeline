# FitTrack Pro ETL Pipeline

## Project Overview
Production-grade ETL pipeline using the Medallion Architecture (Bronze/Silver/Gold) for fitness data. Built with Python 3.10+, DuckDB, Parquet, and Pydantic. Features 6 fictional athletes with 30 days of realistic training, nutrition, and body composition data (2,268+ rows).

## Architecture
- **Bronze** (`src/ingestion/`): Raw data intake from APIs and files → Parquet in `data/bronze/`
- **Silver** (`src/transformation/`): Cleaning, transforms, enrichment → Parquet in `data/silver/`
- **Gold** (`src/warehouse/`): Dimensional modeling (star schema, SCD Type 2) → DuckDB at `data/fittrack.duckdb`
- **Quality** (`src/quality/`): Profiling, validation, anomaly detection, scoring (0-100). Gate: score >= 50 to reach Gold.
- **Monitor** (`src/monitor/`): Alerting, health checks, scheduler, Streamlit dashboard (port 8501)
- **Orchestrator** (`src/orchestrator.py`): Facade coordinating the full pipeline

## Key Commands
```bash
# Run pipeline demo
py -3 run_demo.py

# Run tests
py -3 -m pytest tests/ -v

# Run tests with coverage
py -3 -m pytest tests/ --cov=src --cov-report=term-missing

# Lint
py -3 -m ruff check src/ tests/

# Type check
py -3 -m mypy src/

# Launch monitoring dashboard
py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501

# Docker
docker compose build
docker compose --profile pipeline run --rm pipeline   # Run pipeline
docker compose up dashboard                           # Start dashboard
```

## Conventions
- Config-driven: pipeline settings live in `config/*.yaml`, not hardcoded
- All paths relative to project root
- snake_case for columns, variables, file names
- Type hints on all function signatures
- Docstrings explain WHAT and WHY, not just HOW
- Design patterns: Strategy (ingestors), Facade (orchestrator), Builder (dimensions)
- Data contracts in `src/quality/contracts/` enforce Silver-layer schemas via Pydantic
- Synthetic data generator in `src/utils/data_generator.py` for reproducible demo datasets
- Tests in `tests/` mirror `src/` structure. ~140 tests, all should pass.

## Data Flow
Sources (Wger API, USDA API, CSV/JSON files) → Bronze (raw Parquet) → Silver (cleaned Parquet) → Quality Gate → Gold (DuckDB star schema)

## Dashboard (10 pages)
Overview · Athlete Profiles · Strength Analytics · Nutrition Analytics · Body Composition · Pipeline Runs · Data Quality · Quality Trends · Health Checks · Alerts

## Sample Athletes
Marcus Chen (powerlifter), Priya Sharma (CrossFit), James O'Brien (bodybuilder), Sofia Rodriguez (strength), Tyler Washington (hybrid), Aiko Tanaka (calisthenics)

## Dependencies
Managed via `pyproject.toml`. Core: duckdb, pandas, pyarrow, pydantic, httpx, rich. Dev: pytest, ruff, mypy. Dashboard: streamlit, plotly.
