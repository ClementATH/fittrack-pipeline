# Setup Guide

## Prerequisites

- **Python 3.10+** (this project uses 3.14)
- **pip** (comes with Python)
- **Git** (for version control)

## Installation

### 1. Clone / Navigate to the Project

```bash
cd "C:\Users\cleme\OneDrive\Documents\Claude Data Engineer Work-Pipeline"
```

### 2. Install Dependencies

```bash
# Core dependencies
py -3 -m pip install duckdb pandas pyarrow pydantic pydantic-settings pyyaml requests httpx jinja2 rich apscheduler watchdog

# Dashboard (optional)
py -3 -m pip install streamlit

# Development / Testing (optional)
py -3 -m pip install pytest pytest-cov pytest-mock
```

Or install everything from pyproject.toml:
```bash
py -3 -m pip install -e ".[dev,dashboard]"
```

### 3. Set Up API Keys (Optional)

The USDA FoodData Central API requires a free API key:
1. Go to https://fdc.nal.usda.gov/api-key-signup.html
2. Sign up for a free key
3. Set the environment variable:

```bash
# Windows (Command Prompt)
set USDA_API_KEY=your_key_here

# Windows (PowerShell)
$env:USDA_API_KEY = "your_key_here"

# Linux/Mac
export USDA_API_KEY=your_key_here
```

The Wger API does not require authentication.

### 4. Verify Installation

```bash
py -3 -c "import duckdb, pandas, pyarrow, pydantic, yaml, httpx; print('All dependencies OK')"
```

## Running the Pipeline

### Full Pipeline Run
```bash
py -3 -m src.orchestrator
```

### Run Specific Source Only
```bash
py -3 -m src.orchestrator --source wger_exercises
```

### Quality Checks Only
```bash
py -3 -m src.orchestrator --quality-only
```

### Start the Monitoring Dashboard
```bash
py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501
```
Then open http://localhost:8501 in your browser.

### Run Tests
```bash
py -3 -m pytest tests/ -v
```

### Run Tests with Coverage
```bash
py -3 -m pytest tests/ -v --cov=src --cov-report=term-missing
```

## File Drop Zone

To ingest data from files, drop CSV or JSON files into `data/incoming/`:

```bash
# Copy sample data to test
copy data\sample\sample_workout_log.csv data\incoming\
copy data\sample\sample_body_metrics.csv data\incoming\
```

Then run the pipeline — the file ingestor will pick them up automatically.

## Configuration

All configuration lives in the `config/` directory:

| File | Purpose |
|------|---------|
| `pipeline_config.yaml` | Pipeline settings (paths, retry, logging) |
| `sources.yaml` | Data source definitions (APIs, file patterns) |
| `quality_rules.yaml` | Validation rules (schema, business, freshness) |

## Troubleshooting

**"Module not found" errors:** Make sure you're running from the project root directory and have installed dependencies.

**API connection errors:** Check your internet connection. The Wger API is free and doesn't require auth. For USDA, ensure your API key is set.

**DuckDB lock errors:** Only one process can write to DuckDB at a time. Close the dashboard before running the pipeline, or use read_only mode.
