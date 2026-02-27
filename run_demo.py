"""
FitTrack Pro -- Full Pipeline Demo
====================================

Runs the complete ETL pipeline on sample data to demonstrate
every component working together. Each file type is processed
through its own Bronze -> Silver -> Quality -> Gold pipeline.

Usage: py -3 run_demo.py

LEARN: In a production Medallion Architecture, each dataset
(workouts, body_metrics, nutrition, exercises) flows through its
own pipeline independently. This is critical because:
  1. Each dataset has different columns, so mixing them creates nulls
  2. Quality rules are specific to each dataset
  3. Independent pipelines can fail/succeed independently
  4. Smaller datasets are easier to debug than one giant DataFrame
"""

import json
import shutil
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd

from src.utils.logger import setup_logger, get_logger
from src.utils.config_loader import load_pipeline_config, load_quality_rules, SourceConfig
from src.utils.db_connector import DuckDBConnector
from src.ingestion.file_ingestor import FileIngestor
from src.transformation.cleaner import DataCleaner
from src.transformation.transformer import DataTransformer
from src.transformation.enricher import DataEnricher
from src.warehouse.dim_builder import DimensionBuilder
from src.warehouse.fact_builder import FactBuilder
from src.warehouse.scd_handler import apply_scd_type2
from src.quality.profiler import DataProfiler
from src.quality.validator import DataValidator
from src.quality.anomaly_detector import AnomalyDetector
from src.quality.scorer import QualityScorer
from src.quality.reporter import QualityReporter
from src.monitor.alerter import Alerter
from src.monitor.health_check import HealthChecker


# ================================================================
# Dataset registry: maps source filenames to pipeline configuration
# ================================================================
# LEARN: This registry is a simple way to route each file to the
# correct transformer and enricher. In production, you'd have this
# in a YAML config or database table. The key fields are:
#   - transform_key: source__dataset combo for the Transformer
#   - enrich_key: dataset name for the Enricher
#   - quality_table: table name for quality rules lookup
#   - gold_table: target DuckDB table name
#   - column_map: rename raw columns to match the Gold schema
# ================================================================
DATASET_REGISTRY = {
    "sample_body_metrics.csv": {
        "name": "Body Metrics",
        "transform_source": "file_drop_zone",
        "transform_dataset": "body_metrics",
        "enrich_key": "body_metrics",
        "quality_table": "body_metrics",
        "gold_table": "gold_body_metrics",
        "column_map": {
            "date": "measured_at",
            "athlete_email": "athlete_id",
        },
    },
    "sample_workout_log.csv": {
        "name": "Workout Logs",
        "transform_source": "file_drop_zone",
        "transform_dataset": "workout_logs",
        "enrich_key": "workouts",
        "quality_table": "workouts",
        "gold_table": "gold_workouts",
        "column_map": {
            "date": "workout_date",
            "athlete_email": "athlete_id",
        },
        "defaults": {
            "status": "completed",
        },
    },
    "sample_exercises.json": {
        "name": "Exercises",
        "transform_source": "wger_exercises",
        "transform_dataset": "exercises",
        "enrich_key": "exercises",
        "quality_table": "exercises",
        "gold_table": "gold_exercises",
        "column_map": {},
    },
    "sample_nutrition.json": {
        "name": "Nutrition Logs",
        "transform_source": "file_drop_zone",
        "transform_dataset": "nutrition_logs",
        "enrich_key": "nutrition",
        "quality_table": "nutrition_logs",
        "gold_table": "gold_nutrition_logs",
        "column_map": {
            "athlete_email": "athlete_id",
            "food_name": "meal_name",
        },
    },
}


def banner(text: str) -> None:
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {text}")
    print(f"{'=' * width}")


def section(text: str) -> None:
    print(f"\n--- {text} ---")


def subsection(text: str) -> None:
    print(f"\n  >> {text}")


def prepare_for_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a DataFrame for quality profiling/validation by converting
    unhashable types (lists) and problematic types (numpy booleans)
    to strings. This prevents errors in profiling and deduplication.

    LEARN: Pandas DataFrames can contain Python lists and numpy booleans
    which cause issues with hashing (needed for .duplicated()) and
    numeric operations (numpy bool can't do arithmetic). Converting
    them to strings is a safe pre-processing step before quality checks.
    """
    df = df.copy()
    for col in df.columns:
        # Convert list columns to strings (unhashable type fix)
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(str)
        # Convert boolean columns to strings (numpy bool arithmetic fix)
        if df[col].dtype == "bool":
            df[col] = df[col].astype(str)
    return df


def main():
    start = time.time()
    banner("FITTRACK PRO -- FULL PIPELINE DEMO")
    print("  Processing each dataset independently through Bronze -> Silver -> Gold")

    # ========================================
    # Setup
    # ========================================
    setup_logger("fittrack", log_dir="logs", level="INFO", json_logs=True, console_output=True)
    logger = get_logger("fittrack.demo")
    config = load_pipeline_config()
    quality_rules = load_quality_rules()

    # Initialize all components
    db = DuckDBConnector(config.database.path)
    db.init_warehouse_schema()
    cleaner = DataCleaner()
    transformer = DataTransformer(config.silver.storage_path)
    enricher = DataEnricher()
    dim_builder = DimensionBuilder(config.gold.storage_path)
    fact_builder = FactBuilder(config.gold.storage_path)
    profiler = DataProfiler()
    validator = DataValidator(quality_rules)
    anomaly_detector = AnomalyDetector(
        z_threshold=quality_rules.get("anomaly_detection", {}).get("z_score_threshold", 3.0),
        iqr_multiplier=quality_rules.get("anomaly_detection", {}).get("iqr_multiplier", 1.5),
    )
    scorer = QualityScorer()
    reporter = QualityReporter()
    alerter = Alerter(config.monitoring.alert_log_path)

    # ========================================
    # STEP 0: Health Checks
    # ========================================
    section("STEP 0: HEALTH CHECKS")
    checker = HealthChecker(db_path=config.database.path, data_dir="data", log_dir="logs")
    for result in checker.run_all_checks():
        status = "OK" if result.healthy else "FAIL"
        print(f"  [{status}] {result.name}: {result.message}")

    # ========================================
    # STEP 1: Build Reference Dimensions
    # ========================================
    section("STEP 1: BUILDING REFERENCE DIMENSIONS")

    dim_date = dim_builder.build_dim_date("2025-01-01", "2026-12-31")
    db.load_dataframe("dim_date", dim_date, mode="replace")
    print(f"  dim_date: {len(dim_date)} days -> DuckDB")

    dim_muscles = dim_builder.build_dim_muscle_groups()
    db.load_dataframe("dim_muscle_groups", dim_muscles, mode="replace")
    print(f"  dim_muscle_groups: {len(dim_muscles)} muscle groups -> DuckDB")

    dim_athletes = dim_builder.build_dim_athletes()
    db.load_dataframe("dim_athletes", dim_athletes, mode="replace")
    print(f"  dim_athletes: {len(dim_athletes)} athletes -> DuckDB")

    # ========================================
    # STEP 2: Copy sample files to incoming
    # ========================================
    section("STEP 2: PREPARE FILES FOR INGESTION")
    sample_dir = Path("data/sample")
    incoming_dir = Path("data/incoming")

    # Move any leftover files back from processed (idempotent re-run)
    processed_dir = incoming_dir / "processed"
    if processed_dir.exists():
        for f in processed_dir.iterdir():
            if f.suffix in (".csv", ".json") and f.name.startswith("sample_"):
                shutil.copy2(str(f), str(incoming_dir / f.name))

    # Copy from sample if not already in incoming
    copied = 0
    for f in sample_dir.iterdir():
        if f.suffix in (".csv", ".json"):
            dest = incoming_dir / f.name
            if not dest.exists():
                shutil.copy2(str(f), str(dest))
                copied += 1
                print(f"  Copied: {f.name}")
            else:
                print(f"  Already present: {f.name}")
    if copied == 0:
        print("  All sample files already in data/incoming/")

    # ========================================
    # STEP 3: Bronze -- Ingest Each File Type
    # ========================================
    section("STEP 3: BRONZE -- FILE INGESTION (per dataset)")

    # We ingest files individually to keep datasets separate
    # LEARN: In a real pipeline, you'd either:
    #   (a) Have separate incoming dirs per source, or
    #   (b) Classify files by name pattern, or
    #   (c) Ingest all to Bronze and split by _source_file column
    # We use approach (c): ingest all, then split.

    file_config = SourceConfig(
        type="file",
        description="File drop zone",
        watch_directory="data/incoming",
        supported_formats=[".csv", ".json"],
    )
    file_ingestor = FileIngestor(
        "file_drop_zone", file_config, bronze_path=config.bronze.storage_path
    )
    ingest_result = file_ingestor.ingest()
    print(f"  Status: {ingest_result['status']}")
    print(f"  Total rows ingested: {ingest_result['rows_ingested']}")
    print(f"  Bronze file: {ingest_result.get('file_path', 'N/A')}")

    if ingest_result["rows_ingested"] == 0:
        print("\n  ERROR: No files found in data/incoming/. Check file paths.")
        return

    # Read back the combined bronze data and split by source file
    bronze_df = pd.read_parquet(ingest_result["file_path"])
    source_files = bronze_df["_source_file"].unique()
    print(f"  Source files found: {list(source_files)}")

    # Split into per-dataset DataFrames
    datasets = {}
    for source_file in source_files:
        if source_file in DATASET_REGISTRY:
            file_df = bronze_df[bronze_df["_source_file"] == source_file].copy()
            # Drop columns that are entirely null (from merging different schemas)
            file_df = file_df.dropna(axis=1, how="all")
            datasets[source_file] = file_df
            reg = DATASET_REGISTRY[source_file]
            print(f"  {reg['name']}: {len(file_df)} rows, {len(file_df.columns)} columns")
        else:
            print(f"  WARNING: Unknown file {source_file}, skipping")

    # ========================================
    # STEP 4-6: Process each dataset independently
    # ========================================
    # LEARN: This is the heart of the Medallion Architecture.
    # Each dataset flows independently through:
    #   Silver (clean -> transform -> enrich) ->
    #   Quality (profile -> validate -> detect -> score) ->
    #   Gold (load to DuckDB)
    # If one dataset fails quality, the others still proceed.
    # This is called "dataset-level isolation" -- a critical
    # production pattern that prevents one bad file from
    # blocking the entire pipeline.

    results_summary = []
    run_id = f"demo-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    for source_file, raw_df in datasets.items():
        reg = DATASET_REGISTRY[source_file]

        banner(f"PROCESSING: {reg['name'].upper()}")

        # ── SILVER: Clean ──
        subsection("Silver: Cleaning")
        clean_df = cleaner.clean(raw_df, table_name=reg["quality_table"])
        print(f"    Rows: {len(clean_df)}, Columns: {len(clean_df.columns)}")

        # ── SILVER: Transform ──
        subsection("Silver: Transforming")
        transformed_df = transformer.transform(
            clean_df,
            source=reg["transform_source"],
            dataset=reg["transform_dataset"],
        )
        print(f"    Rows: {len(transformed_df)}, Columns: {len(transformed_df.columns)}")

        # ── SILVER: Enrich ──
        subsection("Silver: Enriching")
        enriched_df = enricher.enrich(transformed_df, dataset=reg["enrich_key"])
        print(f"    Rows: {len(enriched_df)}, Columns: {len(enriched_df.columns)}")

        # ── Apply column mappings ──
        # Map raw column names to schema-expected names
        if reg.get("column_map"):
            for old_col, new_col in reg["column_map"].items():
                if old_col in enriched_df.columns and new_col not in enriched_df.columns:
                    enriched_df = enriched_df.rename(columns={old_col: new_col})
            print(f"    Applied column mappings: {reg['column_map']}")

        # Add default values for missing required columns
        if reg.get("defaults"):
            for col, default_val in reg["defaults"].items():
                if col not in enriched_df.columns:
                    enriched_df[col] = default_val
            print(f"    Added defaults: {reg['defaults']}")

        # Remove duplicate columns (from transform creating renamed copies)
        enriched_df = enriched_df.loc[:, ~enriched_df.columns.duplicated()]

        # Store Silver
        silver_path = transformer.store_silver(
            enriched_df, "file_drop_zone", reg["quality_table"]
        )
        print(f"    Silver file: {silver_path}")

        # ── QUALITY ASSESSMENT ──
        subsection("Quality: Assessment")

        # Prepare for quality checks (convert lists/bools to strings)
        quality_df = prepare_for_quality(enriched_df)

        # Profile
        profile = profiler.profile(quality_df, table_name=reg["quality_table"])
        print(f"    Rows: {profile['summary']['row_count']}")
        print(f"    Nulls: {profile['summary']['null_percentage']}%")
        print(f"    Duplicates: {profile['summary']['duplicate_rows']}")
        print(f"    Warnings: {len(profile['warnings'])}")

        # Validate
        validation_results = validator.validate(quality_df, table_name=reg["quality_table"])
        passed = sum(1 for r in validation_results if r.passed)
        failed = sum(1 for r in validation_results if not r.passed)
        print(f"    Validation: {passed} passed, {failed} failed")
        for r in validation_results:
            icon = "PASS" if r.passed else "FAIL"
            print(f"      [{icon}] {r.rule_name}: {r.message}")

        # Anomaly detection
        anomaly_results = anomaly_detector.detect(quality_df)
        total_anomalies = sum(r.anomaly_count for r in anomaly_results)
        print(f"    Anomalies: {total_anomalies} found across {len(anomaly_results)} checks")

        # Score
        quality_score = scorer.score(
            table_name=reg["quality_table"],
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            null_percentage=profile["summary"]["null_percentage"],
            row_count=len(quality_df),
        )
        print(f"\n    QUALITY SCORE: {quality_score.overall}/100 -- Grade: {quality_score.grade}")
        print(f"      Completeness: {quality_score.completeness}")
        print(f"      Accuracy:     {quality_score.accuracy}")
        print(f"      Consistency:  {quality_score.consistency}")
        print(f"      Timeliness:   {quality_score.timeliness}")

        # Generate report
        report_path = reporter.generate_report(
            table_name=reg["quality_table"],
            quality_score=quality_score,
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            profile=profile,
            run_id=run_id,
        )
        print(f"    Report: {report_path}")

        # ── QUALITY GATE ──
        if quality_score.overall < 50:
            alerter.alert("WARNING", "quality", f"{reg['name']} score too low: {quality_score.overall}")
            print(f"\n    BLOCKED -- Score {quality_score.overall} < 50. Skipping Gold load.")
            results_summary.append({
                "dataset": reg["name"],
                "rows": len(enriched_df),
                "score": quality_score.overall,
                "grade": quality_score.grade,
                "status": "BLOCKED",
                "gold_table": None,
            })
            continue

        print(f"\n    PASSED quality gate ({quality_score.overall} >= 50)")

        # ── GOLD: Load to Warehouse ──
        subsection("Gold: Loading to DuckDB")
        gold_table = reg["gold_table"]
        db.load_dataframe(gold_table, enriched_df, mode="replace")
        print(f"    Loaded {len(enriched_df)} rows into {gold_table}")

        # Store quality score in DuckDB
        score_df = pd.DataFrame([{
            "id": quality_score.id,
            "table_name": quality_score.table_name,
            "run_id": run_id,
            "scored_at": quality_score.scored_at,
            "overall_score": quality_score.overall,
            "completeness_score": quality_score.completeness,
            "accuracy_score": quality_score.accuracy,
            "consistency_score": quality_score.consistency,
            "timeliness_score": quality_score.timeliness,
            "row_count": quality_score.row_count,
            "failed_checks": quality_score.failed_checks,
            "details": json.dumps(quality_score.details),
        }])
        db.load_dataframe("quality_scores", score_df, mode="append")

        results_summary.append({
            "dataset": reg["name"],
            "rows": len(enriched_df),
            "score": quality_score.overall,
            "grade": quality_score.grade,
            "status": "LOADED",
            "gold_table": gold_table,
        })

    # ========================================
    # STEP 6b: Log pipeline runs to DuckDB
    # ========================================
    # LEARN: The dashboard reads from the pipeline_runs table to show
    # run history. Without this, the dashboard shows "No pipeline runs".
    completed_at = datetime.now(timezone.utc).isoformat()
    started_at_iso = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()

    for i, r in enumerate(results_summary):
        run_df = pd.DataFrame([{
            "run_id": f"{run_id}-{i}",
            "pipeline_name": "FitTrack Pro Demo",
            "source_name": r["dataset"],
            "layer": "full",
            "status": "success" if r["status"] == "LOADED" else "blocked",
            "started_at": started_at_iso,
            "completed_at": completed_at,
            "rows_processed": r["rows"],
            "rows_failed": 0 if r["status"] == "LOADED" else r["rows"],
            "error_message": None if r["status"] == "LOADED" else f"Quality score {r['score']} below threshold",
            "metadata": json.dumps({"grade": r["grade"], "score": r["score"], "gold_table": r["gold_table"]}),
        }])
        db.load_dataframe("pipeline_runs", run_df, mode="append")

    print(f"  Logged {len(results_summary)} pipeline run(s) to DuckDB")

    # ========================================
    # STEP 7: Verify the Warehouse
    # ========================================
    section("STEP 7: VERIFY -- QUERY THE WAREHOUSE")

    with db.connection() as conn:
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        print(f"  DuckDB tables ({len(table_names)}):")

        for tbl in sorted(table_names):
            count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"    {tbl}: {count} rows")

        # Run a sample analytical query if we have workout data
        if "gold_body_metrics" in table_names:
            print("\n  Sample query: Body metrics summary")
            result = conn.execute("""
                SELECT
                    COUNT(*) as days_tracked,
                    ROUND(AVG(weight_kg), 1) as avg_weight,
                    ROUND(MIN(weight_kg), 1) as min_weight,
                    ROUND(MAX(weight_kg), 1) as max_weight,
                    ROUND(AVG(body_fat_pct), 1) as avg_body_fat
                FROM gold_body_metrics
            """).fetchdf()
            for col in result.columns:
                print(f"    {col}: {result[col].iloc[0]}")

        if "gold_nutrition_logs" in table_names:
            print("\n  Sample query: Nutrition averages by meal type")
            result = conn.execute("""
                SELECT
                    meal_type,
                    COUNT(*) as meals,
                    ROUND(AVG(calories), 0) as avg_calories,
                    ROUND(AVG(protein_g), 0) as avg_protein_g
                FROM gold_nutrition_logs
                GROUP BY meal_type
                ORDER BY avg_calories DESC
            """).fetchdf()
            print(result.to_string(index=False))

    # ========================================
    # Summary
    # ========================================
    duration = time.time() - start
    banner("PIPELINE DEMO COMPLETE")

    print(f"  Run ID:    {run_id}")
    print(f"  Duration:  {duration:.2f}s")
    print(f"  Datasets:  {len(results_summary)} processed")
    print()
    print(f"  {'Dataset':<20} {'Rows':>6} {'Score':>7} {'Grade':>6} {'Status':>10} {'Gold Table'}")
    print(f"  {'-'*20} {'-'*6} {'-'*7} {'-'*6} {'-'*10} {'-'*25}")
    for r in results_summary:
        gold = r["gold_table"] or "-- blocked --"
        print(f"  {r['dataset']:<20} {r['rows']:>6} {r['score']:>7.1f} {r['grade']:>6} {r['status']:>10} {gold}")

    loaded = sum(1 for r in results_summary if r["status"] == "LOADED")
    blocked = sum(1 for r in results_summary if r["status"] == "BLOCKED")
    print(f"\n  Result: {loaded} loaded to Gold, {blocked} blocked by quality gate")

    print(f"\n  Files processed and moved to: data/incoming/processed/")
    print(f"  Quality reports in: reports/")
    print(f"  DuckDB warehouse: {config.database.path}")
    print(f"\n  Next: py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501")
    print()


if __name__ == "__main__":
    main()
