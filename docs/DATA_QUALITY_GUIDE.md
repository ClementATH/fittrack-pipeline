# Data Quality Guide

## How the Quality Framework Works

Every dataset that passes through the pipeline gets a comprehensive quality assessment. This guide explains each component and how to read the results.

---

## Quality Dimensions (0-100 each)

The framework scores data on four dimensions, weighted to produce an overall score:

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| **Completeness** | 30% | Are required fields present and populated? |
| **Accuracy** | 30% | Do values fall within valid ranges? |
| **Consistency** | 20% | Are formats and enums standardized? |
| **Timeliness** | 20% | Is the data fresh enough? |

**Overall Score** = (Completeness x 0.30) + (Accuracy x 0.30) + (Consistency x 0.20) + (Timeliness x 0.20)

### Grade Scale

| Score | Grade | Meaning |
|-------|-------|---------|
| 95-100 | A+ | Exceptional quality |
| 90-94 | A | High quality |
| 85-89 | B+ | Good quality |
| 80-84 | B | Acceptable quality |
| 70-79 | C | Needs attention |
| 60-69 | D | Poor quality |
| 0-59 | F | Unacceptable — blocked from Gold layer |

---

## Component 1: Data Profiler (`src/quality/profiler.py`)

The profiler generates statistical summaries for every column:

**For numeric columns:** min, max, mean, median, std dev, quartiles, zeros, negatives
**For string columns:** min/max/avg length, empty strings, top 5 most frequent values
**For date columns:** min date, max date, date range
**For boolean columns:** true/false counts and percentages

**Table-level stats:** row count, column count, memory usage, duplicate rows, null percentage

### Reading a Profile

Look for these red flags:
- **Null percentage > 50%** — Column might not be populated correctly
- **Unique count = 1** — Constant column (probably useless)
- **High cardinality strings** — Might be an ID column, not a category

---

## Component 2: Validator (`src/quality/validator.py`)

Validates data against rules defined in `config/quality_rules.yaml`.

### Rule Types

**Schema Rules** — Do the right columns exist with the right types?
```yaml
exercises:
  required_columns: [name, primary_muscle, exercise_type]
  column_types:
    name: "string"
    primary_muscle: "string"
```

**Business Rules** — Do values make domain sense?
```yaml
body_metrics:
  - rule: "weight_reasonable"
    column: "weight_kg"
    check: "range"
    min: 30.0
    max: 300.0
    severity: "WARNING"
```

**Freshness Rules** — Is the data recent enough?
```yaml
workouts:
  max_age_hours: 48
  severity: "WARNING"
```

### Adding New Rules

Edit `config/quality_rules.yaml` — no code changes needed:

```yaml
business_rules:
  your_table:
    - rule: "your_rule_name"
      description: "What this checks"
      column: "column_name"
      check: "range"        # or: in_set, not_empty, min, max
      min: 0
      max: 100
      severity: "WARNING"   # or: CRITICAL, INFO
```

---

## Component 3: Anomaly Detector (`src/quality/anomaly_detector.py`)

Detects statistical outliers using two methods:

### Z-Score Method
- Formula: z = (value - mean) / std_dev
- Flags values where |z| > 3.0 (configurable)
- Best for normally distributed data

### IQR Method (Interquartile Range)
- IQR = Q3 - Q1
- Lower fence = Q1 - 1.5 * IQR
- Upper fence = Q3 + 1.5 * IQR
- More robust for skewed distributions

Both methods run on every numeric column. Results show how many anomalies were found and their statistical details.

---

## Component 4: Quality Reports (`reports/`)

After each pipeline run, a Markdown report is generated in the `reports/` directory:

```
reports/
  quality_report_exercises_20260226_060000.md
  quality_report_body_metrics_20260226_060000.md
```

Each report contains:
1. Overall score and grade
2. Dimension breakdown with visual bars
3. Failed validation checks (sorted by severity)
4. Passed validation checks
5. Anomaly detection results
6. Data profile summary

---

## Quality Gate

The pipeline enforces a minimum quality threshold:
- **Score >= 50**: Data proceeds to Gold layer
- **Score < 50**: Data is BLOCKED with a CRITICAL alert

This prevents corrupt data from entering the analytical layer where dashboards and reports consume it.
