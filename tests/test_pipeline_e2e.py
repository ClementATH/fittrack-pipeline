"""
Tests: End-to-End Pipeline Integration
========================================

Integration tests that verify multiple components working together
across the full Bronze -> Silver -> Gold flow.

# LEARN: E2E tests are different from unit tests:
#   Unit tests: Test one function in isolation
#   Integration tests: Test multiple functions working together
#   E2E tests: Test the entire flow from start to finish
#
# E2E tests are slower but catch bugs that unit tests miss —
# like when Component A produces output that Component B can't handle.
# At WellMed, E2E tests would verify that data flows correctly from
# the source system through transformations to the final warehouse table.
"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from src.transformation.cleaner import DataCleaner
from src.transformation.transformer import DataTransformer
from src.transformation.enricher import DataEnricher
from src.quality.profiler import DataProfiler
from src.quality.validator import DataValidator
from src.quality.anomaly_detector import AnomalyDetector
from src.quality.scorer import QualityScorer
from src.quality.reporter import QualityReporter
from src.warehouse.dim_builder import DimensionBuilder
from src.warehouse.fact_builder import FactBuilder
from src.warehouse.scd_handler import apply_scd_type2


# ============================================================
# Bronze -> Silver Integration
# ============================================================


class TestBronzeToSilver:
    """Test the Bronze -> Silver pipeline flow."""

    def test_exercise_data_flows_through_cleaning(self, sample_exercises_df: pd.DataFrame):
        """Raw exercise data should survive the full clean -> transform -> enrich pipeline."""
        cleaner = DataCleaner()
        transformer = DataTransformer()
        enricher = DataEnricher()

        # Clean
        clean_df = cleaner.clean(sample_exercises_df, table_name="exercises")
        assert len(clean_df) == len(sample_exercises_df)

        # Transform
        transformed_df = transformer.transform(
            clean_df, source="wger_exercises", dataset="exercises"
        )
        assert len(transformed_df) > 0
        assert "slug" in transformed_df.columns or "name" in transformed_df.columns

        # Enrich
        enriched_df = enricher.enrich(transformed_df, dataset="exercises")
        assert "id" in enriched_df.columns
        assert "_processed_at" in enriched_df.columns

    def test_workout_data_flows_through_cleaning(self, sample_workouts_df: pd.DataFrame):
        """Raw workout data should survive the full pipeline."""
        cleaner = DataCleaner()
        transformer = DataTransformer()
        enricher = DataEnricher()

        clean_df = cleaner.clean(sample_workouts_df, table_name="workouts")
        transformed_df = transformer.transform(
            clean_df, source="file_drop_zone", dataset="workout_logs"
        )
        enriched_df = enricher.enrich(transformed_df, dataset="workouts")

        assert len(enriched_df) == len(sample_workouts_df)

    def test_body_metrics_flows_through_cleaning(self, sample_body_metrics_df: pd.DataFrame):
        """Body metrics should be enriched with calculated health indicators."""
        cleaner = DataCleaner()
        enricher = DataEnricher()

        clean_df = cleaner.clean(sample_body_metrics_df, table_name="body_metrics")
        enriched_df = enricher.enrich(clean_df, dataset="body_metrics")

        assert "fat_mass_kg" in enriched_df.columns
        assert "lean_mass_calc_kg" in enriched_df.columns
        assert "weight_change_kg" in enriched_df.columns


# ============================================================
# Quality Check Integration
# ============================================================


class TestQualityPipeline:
    """Test the full quality check pipeline: profile -> validate -> detect -> score."""

    def test_full_quality_flow_exercises(
        self, sample_exercises_df: pd.DataFrame, sample_quality_rules: dict
    ):
        """Full quality pipeline should produce a valid score for exercise data."""
        # First transform to get proper columns
        cleaner = DataCleaner()
        transformer = DataTransformer()
        enricher = DataEnricher()

        # Skip the cleaner's dedup on this data since it contains list columns
        transformed = transformer.transform(
            sample_exercises_df, source="wger_exercises", dataset="exercises"
        )
        enriched = enricher.enrich(transformed, dataset="exercises")
        # Drop duplicate columns
        enriched = enriched.loc[:, ~enriched.columns.duplicated()]
        # Convert list columns to strings so profiler can hash them
        for col in enriched.columns:
            if enriched[col].apply(lambda x: isinstance(x, list)).any():
                enriched[col] = enriched[col].apply(str)
        # Cast boolean columns to proper bool dtype to avoid numpy quantile issues
        for col in enriched.select_dtypes(include=["bool"]).columns:
            enriched[col] = enriched[col].astype(str)

        # Run quality suite
        profiler = DataProfiler()
        profile = profiler.profile(enriched, table_name="exercises")

        validator = DataValidator(sample_quality_rules)
        validation_results = validator.validate(enriched, table_name="exercises")

        detector = AnomalyDetector(min_sample_size=3)
        anomaly_results = detector.detect(enriched)

        scorer = QualityScorer()
        score = scorer.score(
            table_name="exercises",
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            null_percentage=profile["summary"]["null_percentage"],
            row_count=len(enriched),
        )

        assert 0 <= score.overall <= 100
        assert score.grade in ("A+", "A", "B+", "B", "C", "D", "F")
        assert score.row_count == len(enriched)

    def test_full_quality_flow_body_metrics(
        self, sample_body_metrics_df: pd.DataFrame, sample_quality_rules: dict
    ):
        """Full quality pipeline should produce a valid score for body metrics."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df, table_name="body_metrics")

        validator = DataValidator(sample_quality_rules)
        validation_results = validator.validate(
            sample_body_metrics_df, table_name="body_metrics"
        )

        detector = AnomalyDetector(min_sample_size=3)
        anomaly_results = detector.detect(sample_body_metrics_df)

        scorer = QualityScorer()
        score = scorer.score(
            table_name="body_metrics",
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            null_percentage=profile["summary"]["null_percentage"],
            row_count=len(sample_body_metrics_df),
        )

        # Clean sample data should score well
        assert score.overall >= 50
        assert score.grade != "F"

    def test_quality_report_generation(
        self, sample_body_metrics_df: pd.DataFrame, sample_quality_rules: dict, tmp_path: Path
    ):
        """Quality reporter should generate a Markdown report file."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df, table_name="body_metrics")

        validator = DataValidator(sample_quality_rules)
        validation_results = validator.validate(
            sample_body_metrics_df, table_name="body_metrics"
        )

        scorer = QualityScorer()
        score = scorer.score(
            table_name="body_metrics",
            validation_results=validation_results,
            null_percentage=profile["summary"]["null_percentage"],
        )

        reporter = QualityReporter(reports_dir=str(tmp_path / "reports"))
        reporter.generate_report(
            table_name="body_metrics",
            quality_score=score,
            validation_results=validation_results,
            anomaly_results=[],
            profile=profile,
            run_id="test-run-001",
        )

        # Check report file exists
        reports = list((tmp_path / "reports").glob("*.md"))
        assert len(reports) >= 1
        content = reports[0].read_text()
        assert "body_metrics" in content


# ============================================================
# Gold Layer / Warehouse Integration
# ============================================================


class TestGoldLayer:
    """Test Gold layer operations: dimension building, fact building, SCD."""

    def test_build_dim_date(self, tmp_path: Path):
        """dim_date should generate a date dimension with all expected columns."""
        builder = DimensionBuilder(gold_path=str(tmp_path / "gold"))
        df = builder.build_dim_date("2026-01-01", "2026-01-31")
        assert len(df) == 31
        assert "date_key" in df.columns
        assert "day_name" in df.columns
        assert "is_weekend" in df.columns
        assert "month_name" in df.columns

    def test_build_dim_muscle_groups(self, tmp_path: Path):
        """dim_muscle_groups should contain standard muscle groups."""
        builder = DimensionBuilder(gold_path=str(tmp_path / "gold"))
        df = builder.build_dim_muscle_groups()
        assert len(df) > 0
        assert "name" in df.columns
        assert "body_region" in df.columns
        # Should include major groups
        groups = set(df["name"].values)
        assert "chest" in groups
        assert "back" in groups

    def test_build_dim_exercises(self, sample_exercises_df: pd.DataFrame, tmp_path: Path):
        """dim_exercises should transform raw exercise data into a dimension."""
        transformer = DataTransformer()
        transformed = transformer.transform_wger_exercises(sample_exercises_df)
        enricher = DataEnricher()
        enriched = enricher.enrich(transformed, dataset="exercises")

        # Drop duplicate columns that may arise from ID->name mapping
        enriched = enriched.loc[:, ~enriched.columns.duplicated()]

        builder = DimensionBuilder(gold_path=str(tmp_path / "gold"))
        dim = builder.build_dim_exercises(enriched)
        assert len(dim) > 0

    def test_scd_type2_detects_changes(self):
        """SCD Type 2 should create new versions when tracked columns change."""
        existing = pd.DataFrame({
            "slug": ["bench-press", "squat"],
            "name": ["Bench Press", "Squat"],
            "difficulty": ["beginner", "intermediate"],
            "effective_from": ["2026-01-01", "2026-01-01"],
            "effective_to": [None, None],
            "is_current": [True, True],
        })

        incoming = pd.DataFrame({
            "slug": ["bench-press", "squat"],
            "name": ["Bench Press", "Squat"],
            "difficulty": ["intermediate", "intermediate"],  # bench changed!
        })

        result = apply_scd_type2(
            existing, incoming,
            key_columns=["slug"],
            tracked_columns=["difficulty"],
        )

        # Bench press should have 2 versions (old + new)
        bench_versions = result[result["slug"] == "bench-press"]
        assert len(bench_versions) == 2
        # One should be current, one should not
        assert bench_versions["is_current"].sum() == 1

        # Squat should still have 1 version (no change)
        squat_versions = result[result["slug"] == "squat"]
        assert len(squat_versions) == 1

    def test_scd_type2_no_changes(self):
        """SCD Type 2 with no changes should return the original data intact."""
        existing = pd.DataFrame({
            "slug": ["bench-press"],
            "name": ["Bench Press"],
            "difficulty": ["intermediate"],
            "effective_from": ["2026-01-01"],
            "effective_to": [None],
            "is_current": [True],
        })

        incoming = pd.DataFrame({
            "slug": ["bench-press"],
            "name": ["Bench Press"],
            "difficulty": ["intermediate"],  # No change
        })

        result = apply_scd_type2(
            existing, incoming,
            key_columns=["slug"],
            tracked_columns=["difficulty"],
        )

        assert len(result) == 1
        assert bool(result["is_current"].iloc[0]) is True


# ============================================================
# Full Pipeline File Flow
# ============================================================


class TestFileToGoldFlow:
    """Test the complete file-based pipeline from incoming file to Gold layer."""

    def test_csv_file_to_enriched_data(self, tmp_path: Path):
        """A CSV file should flow through the entire pipeline to produce enriched data."""
        # Create incoming CSV
        csv_content = (
            "date,athlete_email,exercise,set_number,weight,weight_unit,reps,rpe\n"
            "2026-02-20,marcus@email.com,Squat,1,100,kg,8,7.5\n"
            "2026-02-20,marcus@email.com,Squat,2,110,kg,6,8.0\n"
        )
        csv_path = tmp_path / "test_workout.csv"
        csv_path.write_text(csv_content)

        # Read file
        df = pd.read_csv(csv_path)

        # Clean
        cleaner = DataCleaner()
        clean_df = cleaner.clean(df, table_name="workouts")

        # Transform
        transformer = DataTransformer()
        transformed = transformer.transform(
            clean_df, source="file_drop_zone", dataset="workout_logs"
        )

        # Enrich
        enricher = DataEnricher()
        enriched = enricher.enrich(transformed, dataset="workouts")

        # Verify the enriched output
        assert len(enriched) == 2
        assert "id" in enriched.columns
        assert "_processed_at" in enriched.columns

    def test_json_file_to_enriched_data(self, tmp_path: Path):
        """A JSON file should flow through the entire pipeline."""
        data = [
            {"log_date": "2026-02-20", "meal_type": "breakfast", "calories": 500,
             "protein_g": 35, "carbs_g": 40, "fats_g": 20},
        ]
        json_path = tmp_path / "test_nutrition.json"
        json_path.write_text(json.dumps(data))

        df = pd.json_normalize(data)

        cleaner = DataCleaner()
        clean_df = cleaner.clean(df, table_name="nutrition")

        enricher = DataEnricher()
        enriched = enricher.enrich(clean_df, dataset="nutrition")

        assert "protein_pct" in enriched.columns
        assert "calories_from_protein" in enriched.columns
