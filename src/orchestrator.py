"""
Pipeline Orchestrator
======================

WHAT: The central brain of the ETL pipeline. Coordinates the full
Bronze -> Silver -> Gold flow for every data source, with quality
checks at each stage.

WHY: Without an orchestrator, you'd have to manually run each step
in the right order. The orchestrator:
  1. Manages execution order (can't transform before ingesting)
  2. Tracks run metadata (start time, row counts, errors)
  3. Handles failures with retry logic
  4. Runs quality checks between layers
  5. Generates reports and alerts

# LEARN: In production, orchestrators like Airflow, Prefect, or
# Dagster handle this. Our orchestrator implements the same patterns:
#   - DAG (Directed Acyclic Graph) of tasks
#   - Retry with exponential backoff
#   - Run logging and metadata tracking
#   - Alerting on failure
# At WellMed, your Databricks jobs or Airflow DAGs serve this role.

Usage:
    python -m src.orchestrator                  # Run full pipeline
    python -m src.orchestrator --source wger    # Run specific source
    python -m src.orchestrator --quality-only   # Run quality checks only
"""

import json
import time
import uuid
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.logger import setup_logger, get_logger
from src.utils.config_loader import (
    load_pipeline_config,
    load_source_configs,
    load_quality_rules,
)
from src.utils.db_connector import DuckDBConnector
from src.ingestion.api_ingestor import APIIngestor
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

logger = get_logger("fittrack.orchestrator")


class PipelineOrchestrator:
    """
    Orchestrates the full ETL pipeline: Ingest -> Clean -> Transform ->
    Enrich -> Quality Check -> Load to Gold.

    # LEARN: This class follows the "Facade Pattern" — it provides a
    # simple interface (run_pipeline()) that hides the complexity of
    # coordinating 10+ different modules. The caller doesn't need to
    # know about cleaners, transformers, or validators — they just
    # call run_pipeline() and get results.
    """

    def __init__(self):
        # Load all configuration
        self.config = load_pipeline_config()
        self.source_configs = load_source_configs()
        self.quality_rules = load_quality_rules()

        # Set up logging
        setup_logger(
            name="fittrack",
            log_dir=self.config.logging.log_dir,
            level=self.config.logging.level,
            json_logs=self.config.logging.json_logs,
            console_output=self.config.logging.console_output,
        )

        # Initialize components
        self.db = DuckDBConnector(self.config.database.path)
        self.cleaner = DataCleaner(self.config.silver.naming_convention)
        self.transformer = DataTransformer(self.config.silver.storage_path)
        self.enricher = DataEnricher()
        self.dim_builder = DimensionBuilder(self.config.gold.storage_path)
        self.fact_builder = FactBuilder(self.config.gold.storage_path)
        self.profiler = DataProfiler()
        self.validator = DataValidator(self.quality_rules)
        self.anomaly_detector = AnomalyDetector(
            z_threshold=self.quality_rules.get("anomaly_detection", {}).get("z_score_threshold", 3.0),
            iqr_multiplier=self.quality_rules.get("anomaly_detection", {}).get("iqr_multiplier", 1.5),
        )
        self.scorer = QualityScorer()
        self.reporter = QualityReporter()
        self.alerter = Alerter(self.config.monitoring.alert_log_path)

        # Initialize warehouse schema
        self.db.init_warehouse_schema()

        logger.info("Pipeline orchestrator initialized")

    # ============================================================
    # Main Pipeline Entry Points
    # ============================================================

    def run_full_pipeline(self, source_filter: str | None = None) -> dict[str, Any]:
        """
        Run the complete ETL pipeline for all (or filtered) sources.

        Args:
            source_filter: If provided, only run this specific source.

        Returns:
            Summary dictionary with results for each source.
        """
        run_id = str(uuid.uuid4())
        start_time = datetime.now(timezone.utc)

        logger.info(
            f"=== PIPELINE RUN STARTED === (run_id: {run_id[:8]})",
            extra={"run_id": run_id},
        )

        # Health check first
        self._run_health_checks(run_id)

        results: dict[str, Any] = {
            "run_id": run_id,
            "started_at": start_time.isoformat(),
            "sources": {},
        }

        # Build reference dimensions first (date, muscle groups)
        self._build_reference_dimensions()

        # Process each source through Bronze -> Silver -> Gold
        for source_name, source_config in self.source_configs.items():
            if source_filter and source_name != source_filter:
                continue

            source_result = self._process_source(source_name, source_config, run_id)
            results["sources"][source_name] = source_result

        # Final summary
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        results["completed_at"] = end_time.isoformat()
        results["duration_seconds"] = round(duration, 2)

        # Count totals
        total_rows = sum(
            r.get("rows_processed", 0) for r in results["sources"].values()
        )
        total_errors = sum(
            1 for r in results["sources"].values() if r.get("status") == "error"
        )

        results["total_rows_processed"] = total_rows
        results["total_errors"] = total_errors

        logger.info(
            f"=== PIPELINE RUN COMPLETE === "
            f"Duration: {duration:.1f}s | Rows: {total_rows} | Errors: {total_errors}",
            extra={"run_id": run_id},
        )

        # Save run log
        self._save_run_log(results)

        return results

    def run_quality_only(self) -> dict[str, Any]:
        """Run quality checks on existing Silver layer data without re-ingesting."""
        run_id = str(uuid.uuid4())
        logger.info(f"Running quality-only checks (run_id: {run_id[:8]})")

        results: dict[str, list] = {"quality_scores": []}
        silver_path = Path(self.config.silver.storage_path)

        for parquet_file in silver_path.rglob("*.parquet"):
            table_name = parquet_file.stem
            try:
                df = pd.read_parquet(parquet_file)
                score = self._run_quality_checks(df, table_name, run_id)
                results["quality_scores"].append(score.to_dict())
            except Exception as e:
                logger.error(f"Quality check failed for {table_name}: {e}")

        return results

    # ============================================================
    # Source Processing Pipeline
    # ============================================================

    def _process_source(
        self,
        source_name: str,
        source_config: Any,
        run_id: str,
    ) -> dict[str, Any]:
        """
        Process a single data source through the full pipeline.

        Flow: Bronze (ingest) -> Silver (clean+transform+enrich) ->
              Quality Check -> Gold (load)
        """
        result: dict[str, Any] = {
            "source": source_name,
            "status": "error",
            "rows_processed": 0,
            "endpoints": {},
        }

        try:
            # Create the appropriate ingestor
            ingestor = self._create_ingestor(source_name, source_config)
            if ingestor is None:
                result["error"] = f"Unknown source type: {source_config.type}"
                return result

            # Process each endpoint
            endpoints = list(source_config.endpoints.keys()) if source_config.endpoints else ["default"]

            for endpoint_name in endpoints:
                ep_result = self._process_endpoint(
                    ingestor, source_name, endpoint_name, run_id
                )
                result["endpoints"][endpoint_name] = ep_result
                result["rows_processed"] += ep_result.get("rows_processed", 0)

            result["status"] = "success"

        except Exception as e:
            result["error"] = str(e)
            self.alerter.alert(
                "CRITICAL", source_name, f"Source processing failed: {e}"
            )
            logger.error(f"Source {source_name} failed: {e}", exc_info=True)

        # Log the run to the database
        self._log_pipeline_run(run_id, source_name, "full", result)

        return result

    def _process_endpoint(
        self,
        ingestor: Any,
        source_name: str,
        endpoint_name: str,
        run_id: str,
    ) -> dict[str, Any]:
        """Process a single endpoint through Bronze -> Silver -> Gold."""
        ep_result: dict[str, Any] = {
            "endpoint": endpoint_name,
            "status": "error",
            "rows_processed": 0,
        }

        try:
            # ── BRONZE: Ingest raw data ──
            logger.info(f"[BRONZE] Ingesting {source_name}/{endpoint_name}")
            ingest_result = ingestor.ingest(endpoint_name=endpoint_name)

            if ingest_result["status"] != "success" or ingest_result["rows_ingested"] == 0:
                ep_result["status"] = "skipped"
                ep_result["reason"] = "No data or ingestion error"
                return ep_result

            # Read the Bronze file back
            bronze_path = ingest_result["file_path"]
            raw_df = pd.read_parquet(bronze_path)
            logger.info(f"[BRONZE] Loaded {len(raw_df)} rows from {bronze_path}")

            # ── SILVER: Clean -> Transform -> Enrich ──
            logger.info(f"[SILVER] Processing {source_name}/{endpoint_name}")

            # Step 1: Clean
            clean_df = self.cleaner.clean(raw_df, table_name=endpoint_name)

            # Step 2: Transform (source-specific)
            transformed_df = self.transformer.transform(
                clean_df, source=source_name, dataset=endpoint_name
            )

            # Step 3: Enrich
            dataset_type = self._map_endpoint_to_dataset(endpoint_name)
            enriched_df = self.enricher.enrich(transformed_df, dataset=dataset_type)

            # Store Silver
            silver_path = self.transformer.store_silver(
                enriched_df, source_name, endpoint_name
            )

            # ── QUALITY CHECK ──
            quality_score = self._run_quality_checks(
                enriched_df, endpoint_name, run_id
            )

            # Only proceed to Gold if quality passes threshold
            if quality_score.overall < 50:
                self.alerter.alert(
                    "CRITICAL",
                    source_name,
                    f"Quality score too low for {endpoint_name}: "
                    f"{quality_score.overall}/100 ({quality_score.grade})",
                )
                ep_result["status"] = "quality_failed"
                ep_result["quality_score"] = quality_score.overall
                return ep_result

            # ── GOLD: Build dimensions/facts ──
            logger.info(f"[GOLD] Loading {source_name}/{endpoint_name}")
            self._load_to_gold(enriched_df, source_name, endpoint_name)

            # ── Load into DuckDB ──
            table_name = f"gold_{endpoint_name}"
            self.db.load_dataframe(table_name, enriched_df, mode="replace")

            ep_result["status"] = "success"
            ep_result["rows_processed"] = len(enriched_df)
            ep_result["quality_score"] = quality_score.overall
            ep_result["quality_grade"] = quality_score.grade

        except Exception as e:
            ep_result["error"] = str(e)
            self.alerter.alert(
                "WARNING",
                source_name,
                f"Endpoint {endpoint_name} failed: {e}",
            )
            logger.error(
                f"Endpoint {source_name}/{endpoint_name} failed: {e}",
                exc_info=True,
            )

        return ep_result

    # ============================================================
    # Helper Methods
    # ============================================================

    def _create_ingestor(self, source_name: str, source_config: Any) -> Any:
        """Create the appropriate ingestor based on source type."""
        if source_config.type == "rest_api":
            return APIIngestor(
                source_name,
                source_config,
                bronze_path=self.config.bronze.storage_path,
                max_retries=self.config.retry.max_attempts,
                base_delay=self.config.retry.base_delay_seconds,
                backoff_factor=self.config.retry.backoff_factor,
            )
        elif source_config.type == "file":
            return FileIngestor(
                source_name,
                source_config,
                bronze_path=self.config.bronze.storage_path,
            )
        return None

    def _map_endpoint_to_dataset(self, endpoint_name: str) -> str:
        """Map an endpoint name to a dataset type for enrichment."""
        mapping = {
            "exercises": "exercises",
            "muscles": "exercises",
            "equipment": "exercises",
            "foods_search": "nutrition",
            "food_detail": "nutrition",
            "workout_logs": "workouts",
            "body_metrics": "body_metrics",
            "nutrition_logs": "nutrition",
        }
        return mapping.get(endpoint_name, endpoint_name)

    def _load_to_gold(
        self, df: pd.DataFrame, source_name: str, endpoint_name: str
    ) -> None:
        """Load data into Gold layer (dimensions or facts based on endpoint)."""
        dim_endpoints = {"exercises", "muscles", "equipment"}
        fact_endpoints = {"workouts", "workout_logs", "body_metrics", "nutrition_logs", "foods_search"}

        if endpoint_name in dim_endpoints:
            # Try SCD Type 2 if existing dimension exists
            existing = None
            dim_path = Path(self.config.gold.storage_path) / "dim_exercises.parquet"
            if dim_path.exists():
                existing = pd.read_parquet(dim_path)

            if existing is not None and "slug" in df.columns:
                result = apply_scd_type2(
                    existing, df,
                    key_columns=["slug"],
                    tracked_columns=["name", "primary_muscle", "equipment", "difficulty"],
                )
                self.dim_builder._store_dimension(result, "dim_exercises")
            else:
                self.dim_builder.build_dim_exercises(df)

        elif endpoint_name in {"workout_logs", "workouts"}:
            self.fact_builder.build_fact_workouts(df)

        elif endpoint_name == "body_metrics":
            self.fact_builder.build_fact_body_metrics(df)

        elif endpoint_name in {"nutrition_logs", "foods_search"}:
            self.fact_builder.build_fact_nutrition(df)

    def _build_reference_dimensions(self) -> None:
        """Build reference dimensions that don't depend on source data."""
        logger.info("Building reference dimensions...")
        self.dim_builder.build_dim_date("2025-01-01", "2026-12-31")
        self.dim_builder.build_dim_muscle_groups()
        self.dim_builder.build_dim_athletes()

    def _run_quality_checks(
        self, df: pd.DataFrame, table_name: str, run_id: str
    ) -> Any:
        """Run full quality suite: profile -> validate -> anomaly -> score -> report."""
        # Profile
        profile = self.profiler.profile(df, table_name=table_name)

        # Validate
        validation_results = self.validator.validate(df, table_name=table_name)

        # Anomaly detection on numeric columns
        monitor_cols = self.quality_rules.get("anomaly_detection", {}).get(
            "columns_to_monitor", {}
        ).get(table_name, [])
        anomaly_results = self.anomaly_detector.detect(
            df, columns=monitor_cols if monitor_cols else None
        )

        # Score
        null_pct = profile["summary"]["null_percentage"]
        quality_score = self.scorer.score(
            table_name=table_name,
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            null_percentage=null_pct,
            row_count=len(df),
        )

        # Generate report
        self.reporter.generate_report(
            table_name=table_name,
            quality_score=quality_score,
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            profile=profile,
            run_id=run_id,
        )

        # Store quality score in DuckDB
        try:
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
            self.db.load_dataframe("quality_scores", score_df, mode="append")
        except Exception as e:
            logger.warning(f"Failed to store quality score: {e}")

        # Alert on low quality
        if quality_score.overall < 70:
            self.alerter.alert(
                "WARNING",
                "quality",
                f"{table_name} quality score: {quality_score.overall}/100 ({quality_score.grade})",
                {"score": quality_score.to_dict()},
            )

        return quality_score

    def _run_health_checks(self, run_id: str) -> None:
        """Run system health checks before pipeline execution."""
        checker = HealthChecker(
            db_path=self.config.database.path,
            data_dir="data",
            log_dir=self.config.logging.log_dir,
        )
        results = checker.run_all_checks()

        unhealthy = [r for r in results if not r.healthy]
        if unhealthy:
            for r in unhealthy:
                self.alerter.alert(
                    "WARNING",
                    "health_check",
                    f"Health check failed: {r.name} — {r.message}",
                )

    def _log_pipeline_run(
        self, run_id: str, source_name: str, layer: str, result: dict
    ) -> None:
        """Log a pipeline run to the database."""
        try:
            run_df = pd.DataFrame([{
                "run_id": run_id,
                "pipeline_name": self.config.name,
                "source_name": source_name,
                "layer": layer,
                "status": result.get("status", "error"),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "rows_processed": result.get("rows_processed", 0),
                "rows_failed": 0,
                "error_message": result.get("error"),
                "metadata": json.dumps({"endpoints": list(result.get("endpoints", {}).keys())}),
            }])
            self.db.load_dataframe("pipeline_runs", run_df, mode="append")
        except Exception as e:
            logger.warning(f"Failed to log pipeline run: {e}")

    def _save_run_log(self, results: dict) -> None:
        """Save complete run results as a JSON log file."""
        log_dir = Path(self.config.logging.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"pipeline_run_{timestamp}.json"

        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

        logger.info(f"Run log saved: {log_path}")


# ============================================================
# CLI Entry Point
# ============================================================

def main():
    """Command-line entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="FitTrack Pro ETL Pipeline")
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Run only a specific source (e.g., 'wger_exercises')",
    )
    parser.add_argument(
        "--quality-only",
        action="store_true",
        help="Run quality checks only (no ingestion)",
    )
    args = parser.parse_args()

    orchestrator = PipelineOrchestrator()

    if args.quality_only:
        results = orchestrator.run_quality_only()
    else:
        results = orchestrator.run_full_pipeline(source_filter=args.source)

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE RUN SUMMARY")
    print("=" * 60)
    print(f"  Run ID:    {results.get('run_id', 'N/A')[:8]}")
    print(f"  Duration:  {results.get('duration_seconds', 0):.1f}s")
    print(f"  Rows:      {results.get('total_rows_processed', 0):,}")
    print(f"  Errors:    {results.get('total_errors', 0)}")

    for source, data in results.get("sources", {}).items():
        status_icon = "OK" if data.get("status") == "success" else "FAIL"
        print(f"  [{status_icon}] {source}: {data.get('rows_processed', 0)} rows")

    print("=" * 60)


if __name__ == "__main__":
    main()
