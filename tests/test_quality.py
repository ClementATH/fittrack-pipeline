"""
Tests: Data Quality Framework
===============================

Tests for profiling, validation, anomaly detection, and scoring.

# LEARN: Quality framework tests ensure that bad data gets caught
# BEFORE it reaches the Gold layer. These tests verify that our
# "safety net" actually works — that validation catches invalid values,
# anomaly detection flags outliers, and scoring produces meaningful grades.
"""

from typing import Any

import pandas as pd
import pytest

from src.quality.profiler import DataProfiler
from src.quality.validator import DataValidator, ValidationResult
from src.quality.anomaly_detector import AnomalyDetector, AnomalyResult
from src.quality.scorer import QualityScorer, QualityScore


# ============================================================
# DataProfiler Tests
# ============================================================


class TestDataProfiler:
    """Test the statistical profiler."""

    def test_profile_returns_required_keys(self, sample_body_metrics_df: pd.DataFrame):
        """Profile output should have summary, columns, and warnings."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df, table_name="body_metrics")
        assert "summary" in profile
        assert "columns" in profile
        assert "warnings" in profile
        assert profile["table_name"] == "body_metrics"

    def test_profile_summary_row_count(self, sample_body_metrics_df: pd.DataFrame):
        """Summary should report correct row count."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df)
        assert profile["summary"]["row_count"] == len(sample_body_metrics_df)

    def test_profile_summary_column_count(self, sample_body_metrics_df: pd.DataFrame):
        """Summary should report correct column count."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df)
        assert profile["summary"]["column_count"] == len(sample_body_metrics_df.columns)

    def test_profile_numeric_column_stats(self):
        """Numeric columns should have min, max, mean, median, std."""
        df = pd.DataFrame({"weight": [60.0, 70.0, 80.0, 90.0, 100.0]})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        col_stats = profile["columns"]["weight"]
        assert col_stats["min"] == 60.0
        assert col_stats["max"] == 100.0
        assert col_stats["mean"] == pytest.approx(80.0)

    def test_profile_string_column_stats(self):
        """String columns should have length stats and top values."""
        df = pd.DataFrame({"name": ["Squat", "Bench", "Bench", "Deadlift"]})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        col_stats = profile["columns"]["name"]
        assert "min_length" in col_stats
        assert "max_length" in col_stats
        assert "top_values" in col_stats
        assert "Bench" in col_stats["top_values"]

    def test_profile_null_percentage(self):
        """Profile should correctly report null percentages."""
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, None]})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        assert profile["columns"]["a"]["null_pct"] == pytest.approx(33.33, abs=1)
        assert profile["columns"]["b"]["null_pct"] == 100.0

    def test_profile_warning_high_nulls(self):
        """Columns with >50% nulls should generate a WARNING."""
        df = pd.DataFrame({"a": [1, None, None, None]})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        warning_cols = [w["column"] for w in profile["warnings"]]
        assert "a" in warning_cols

    def test_profile_warning_constant_column(self):
        """Columns with only one unique value should generate INFO warning."""
        df = pd.DataFrame({"status": ["active", "active", "active"]})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        info_warnings = [w for w in profile["warnings"] if w["severity"] == "INFO"]
        constant_warnings = [w for w in info_warnings if "constant" in w["warning"].lower()]
        assert len(constant_warnings) > 0

    def test_profile_to_markdown(self, sample_body_metrics_df: pd.DataFrame):
        """profile_to_markdown should produce valid Markdown output."""
        profiler = DataProfiler()
        profile = profiler.profile(sample_body_metrics_df, "body_metrics")
        md = profiler.profile_to_markdown(profile)
        assert "# Data Profile: body_metrics" in md
        assert "## Summary" in md
        assert "## Column Details" in md

    def test_profile_empty_dataframe(self):
        """Profiling an empty DataFrame should not crash."""
        df = pd.DataFrame({"a": pd.Series(dtype="float64")})
        profiler = DataProfiler()
        profile = profiler.profile(df)
        assert profile["summary"]["row_count"] == 0


# ============================================================
# DataValidator Tests
# ============================================================


class TestDataValidator:
    """Test YAML-driven data validation."""

    def test_validate_schema_required_columns_present(
        self, sample_quality_rules: dict[str, Any]
    ):
        """When all required columns are present, schema check should pass."""
        df = pd.DataFrame({
            "name": ["Squat"],
            "primary_muscle": ["quads"],
            "exercise_type": ["compound"],
        })
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(df, table_name="exercises")
        schema_results = [r for r in results if r.rule_name.startswith("schema_required")]
        assert all(r.passed for r in schema_results)

    def test_validate_schema_missing_column(
        self, sample_quality_rules: dict[str, Any]
    ):
        """Missing required columns should fail schema validation."""
        df = pd.DataFrame({"name": ["Squat"]})  # Missing primary_muscle, exercise_type
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(df, table_name="exercises")
        schema_results = [r for r in results if r.rule_name.startswith("schema_required")]
        failed = [r for r in schema_results if not r.passed]
        assert len(failed) >= 2

    def test_validate_business_rule_not_empty(
        self, sample_quality_rules: dict[str, Any]
    ):
        """not_empty check should fail when column has empty strings."""
        df = pd.DataFrame({
            "name": ["Squat", "", "Bench"],
            "primary_muscle": ["quads", "chest", "chest"],
            "exercise_type": ["compound", "compound", "compound"],
        })
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(df, table_name="exercises")
        name_result = [r for r in results if r.rule_name == "name_not_empty"]
        assert len(name_result) == 1
        assert not name_result[0].passed
        assert name_result[0].failing_rows == 1

    def test_validate_business_rule_in_set(
        self, sample_quality_rules: dict[str, Any]
    ):
        """in_set check should fail for values not in the allowed set."""
        df = pd.DataFrame({
            "name": ["Squat"],
            "primary_muscle": ["quads"],
            "exercise_type": ["INVALID_TYPE"],
        })
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(df, table_name="exercises")
        set_result = [r for r in results if r.rule_name == "valid_exercise_type"]
        assert len(set_result) == 1
        assert not set_result[0].passed

    def test_validate_business_rule_range(
        self, sample_quality_rules: dict[str, Any]
    ):
        """Range check should flag values outside min/max bounds."""
        df = pd.DataFrame({
            "weight_kg": [80.0, 500.0, 25.0],  # 500 > 300, 25 < 30
            "body_fat_pct": [15.0, 20.0, 25.0],
        })
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(df, table_name="body_metrics")
        weight_result = [r for r in results if r.rule_name == "weight_reasonable"]
        assert len(weight_result) == 1
        assert not weight_result[0].passed
        assert weight_result[0].failing_rows == 2  # 500 and 25

    def test_validate_business_rule_range_all_valid(
        self, sample_quality_rules: dict[str, Any], sample_body_metrics_df: pd.DataFrame
    ):
        """All sample body metrics should pass range checks."""
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(sample_body_metrics_df, table_name="body_metrics")
        range_results = [r for r in results if r.rule_name in ("weight_reasonable", "body_fat_reasonable")]
        assert all(r.passed for r in range_results)

    def test_validate_freshness_recent_data(
        self, sample_quality_rules: dict[str, Any], sample_body_metrics_df: pd.DataFrame
    ):
        """Recent data should pass the freshness check."""
        validator = DataValidator(sample_quality_rules)
        results = validator.validate(sample_body_metrics_df, table_name="body_metrics")
        freshness_results = [r for r in results if "freshness" in r.rule_name]
        # Sample data has dates from today/yesterday so should be fresh
        assert len(freshness_results) >= 1

    def test_validate_no_rules_returns_empty(self):
        """Validating a table with no rules should return empty results."""
        validator = DataValidator({})
        df = pd.DataFrame({"a": [1]})
        results = validator.validate(df, table_name="nonexistent_table")
        assert len(results) == 0

    def test_validation_result_to_dict(self):
        """ValidationResult.to_dict() should produce a clean dictionary."""
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            passed=True,
            severity="INFO",
            message="Test passed",
        )
        d = result.to_dict()
        assert d["rule_name"] == "test_rule"
        assert d["passed"] is True
        assert "checked_at" in d


# ============================================================
# AnomalyDetector Tests
# ============================================================


class TestAnomalyDetector:
    """Test statistical anomaly detection methods."""

    def test_zscore_detects_obvious_outlier(self):
        """Z-score should flag a value that's clearly an outlier."""
        # 10 normal values and 1 extreme outlier
        data = [80, 81, 79, 80, 82, 78, 80, 81, 79, 80, 200]
        df = pd.DataFrame({"weight": data})
        detector = AnomalyDetector(z_threshold=2.0, min_sample_size=5)
        results = detector.detect(df, columns=["weight"])
        z_results = [r for r in results if r.method == "z_score"]
        assert len(z_results) == 1
        assert z_results[0].anomaly_count >= 1

    def test_iqr_detects_obvious_outlier(self):
        """IQR should flag values outside the fences."""
        data = [80, 81, 79, 80, 82, 78, 80, 81, 79, 80, 200]
        df = pd.DataFrame({"weight": data})
        detector = AnomalyDetector(iqr_multiplier=1.5, min_sample_size=5)
        results = detector.detect(df, columns=["weight"])
        iqr_results = [r for r in results if r.method == "iqr"]
        assert len(iqr_results) == 1
        assert iqr_results[0].anomaly_count >= 1

    def test_no_anomalies_in_normal_data(self):
        """Normal, well-distributed data should have zero anomalies."""
        data = list(range(50, 100))
        df = pd.DataFrame({"value": data})
        detector = AnomalyDetector(z_threshold=3.0, min_sample_size=5)
        results = detector.detect(df, columns=["value"])
        total_anomalies = sum(r.anomaly_count for r in results)
        assert total_anomalies == 0

    def test_min_sample_size_skips_small_data(self):
        """Columns with fewer than min_sample_size values should be skipped."""
        df = pd.DataFrame({"tiny": [1, 2, 3]})
        detector = AnomalyDetector(min_sample_size=10)
        results = detector.detect(df, columns=["tiny"])
        assert len(results) == 0

    def test_auto_detect_numeric_columns(self):
        """Without specifying columns, should auto-detect numeric columns."""
        df = pd.DataFrame({
            "weight": list(range(20)),
            "name": ["a"] * 20,
            "reps": list(range(20)),
        })
        detector = AnomalyDetector(min_sample_size=5)
        results = detector.detect(df)
        checked_cols = {r.column for r in results}
        assert "weight" in checked_cols
        assert "reps" in checked_cols
        assert "name" not in checked_cols

    def test_constant_column_no_anomalies(self):
        """A constant column (std=0) should report zero anomalies."""
        df = pd.DataFrame({"constant": [5.0] * 20})
        detector = AnomalyDetector(min_sample_size=5)
        results = detector.detect(df, columns=["constant"])
        assert all(r.anomaly_count == 0 for r in results)

    def test_anomaly_result_to_dict(self):
        """AnomalyResult.to_dict() should produce a valid dictionary."""
        result = AnomalyResult(
            column="test", method="z_score",
            anomaly_count=2, total_count=100, threshold=3.0,
        )
        d = result.to_dict()
        assert d["column"] == "test"
        assert d["anomaly_pct"] == 2.0


# ============================================================
# QualityScorer Tests
# ============================================================


class TestQualityScorer:
    """Test the quality scoring engine."""

    def test_perfect_score_with_no_issues(self):
        """With no failures, the score should be 100 (or close)."""
        scorer = QualityScorer()
        score = scorer.score(
            table_name="test",
            validation_results=[],
            anomaly_results=[],
            null_percentage=0.0,
            row_count=100,
        )
        assert score.overall == 100.0
        assert score.grade == "A+"

    def test_null_penalty(self):
        """High null percentage should reduce the completeness score."""
        scorer = QualityScorer()
        score = scorer.score(
            table_name="test",
            validation_results=[],
            anomaly_results=[],
            null_percentage=25.0,
            row_count=100,
        )
        assert score.completeness < 100.0
        assert score.overall < 100.0

    def test_grade_scale(self):
        """Grade boundaries should be correctly applied."""
        score = QualityScore("test", overall=96, completeness=96, accuracy=96, consistency=96, timeliness=96)
        assert score.grade == "A+"

        score = QualityScore("test", overall=90, completeness=90, accuracy=90, consistency=90, timeliness=90)
        assert score.grade == "A"

        score = QualityScore("test", overall=85, completeness=85, accuracy=85, consistency=85, timeliness=85)
        assert score.grade == "B+"

        score = QualityScore("test", overall=75, completeness=75, accuracy=75, consistency=75, timeliness=75)
        assert score.grade == "C"

        score = QualityScore("test", overall=45, completeness=45, accuracy=45, consistency=45, timeliness=45)
        assert score.grade == "F"

    def test_failed_validation_reduces_accuracy(self):
        """Business rule failures should reduce the accuracy score."""
        failed_result = ValidationResult(
            rule_name="test_range",
            table_name="test",
            passed=False,
            severity="WARNING",
            message="Out of range",
            failing_rows=10,
            total_rows=100,
        )
        scorer = QualityScorer()
        score = scorer.score(
            table_name="test",
            validation_results=[failed_result],
            anomaly_results=[],
            null_percentage=0.0,
        )
        assert score.accuracy < 100.0

    def test_missing_schema_columns_reduce_completeness(self):
        """Missing required columns should heavily penalize completeness."""
        missing_result = ValidationResult(
            rule_name="schema_required_column_name",
            table_name="test",
            passed=False,
            severity="CRITICAL",
            message="Missing column",
        )
        scorer = QualityScorer()
        score = scorer.score(
            table_name="test",
            validation_results=[missing_result],
        )
        assert score.completeness < 100.0

    def test_anomalies_reduce_accuracy(self):
        """Anomaly detection findings should reduce the accuracy score."""
        anomaly = AnomalyResult(
            column="weight",
            method="z_score",
            anomaly_count=5,
            total_count=100,
            threshold=3.0,
        )
        scorer = QualityScorer()
        score = scorer.score(
            table_name="test",
            anomaly_results=[anomaly],
        )
        assert score.accuracy < 100.0

    def test_score_to_dict(self):
        """QualityScore.to_dict() should produce a complete dictionary."""
        score = QualityScore("test", 85.0, 90.0, 80.0, 85.0, 85.0, row_count=100)
        d = score.to_dict()
        assert d["table_name"] == "test"
        assert d["overall_score"] == 85.0
        assert d["grade"] == "B+"
        assert "scored_at" in d

    def test_timeliness_perfect_when_no_freshness_rules(self):
        """Without freshness checks, timeliness should be 100."""
        scorer = QualityScorer()
        score = scorer.score(table_name="test")
        assert score.timeliness == 100.0

    def test_weights_sum_to_one(self):
        """Quality dimension weights should sum to exactly 1.0."""
        total = sum(QualityScorer.WEIGHTS.values())
        assert total == pytest.approx(1.0)
