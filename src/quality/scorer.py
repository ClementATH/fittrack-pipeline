"""
Data Quality Scorer
====================

WHAT: Computes a 0-100 quality score for each table, broken down by
four dimensions: Completeness, Accuracy, Consistency, and Timeliness.

WHY: A single number (0-100) makes it immediately clear whether data
quality is acceptable or needs attention. Stakeholders don't want to
read 50 validation results — they want "is this data trustworthy?"

# LEARN: The four quality dimensions come from industry standards:
#   - Completeness: Are all expected fields populated?
#   - Accuracy: Do values fall within valid ranges?
#   - Consistency: Are formats and values standardized?
#   - Timeliness: Is the data fresh enough?
#
# This is the same framework used at companies like Google, Netflix,
# and healthcare systems subject to CMS data quality requirements.
# At WellMed, data quality scores could be required for CMS reporting.
"""

import uuid
from datetime import datetime, timezone
from typing import Any

from src.quality.validator import ValidationResult
from src.quality.anomaly_detector import AnomalyResult
from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.scorer")


class QualityScore:
    """Represents a quality score for a single table."""

    def __init__(
        self,
        table_name: str,
        overall: float,
        completeness: float,
        accuracy: float,
        consistency: float,
        timeliness: float,
        row_count: int = 0,
        failed_checks: int = 0,
        details: dict[str, Any] | None = None,
    ):
        self.id = str(uuid.uuid4())
        self.table_name = table_name
        self.overall = round(overall, 2)
        self.completeness = round(completeness, 2)
        self.accuracy = round(accuracy, 2)
        self.consistency = round(consistency, 2)
        self.timeliness = round(timeliness, 2)
        self.row_count = row_count
        self.failed_checks = failed_checks
        self.details = details or {}
        self.scored_at = datetime.now(timezone.utc).isoformat()

    @property
    def grade(self) -> str:
        """Letter grade based on overall score."""
        if self.overall >= 95:
            return "A+"
        elif self.overall >= 90:
            return "A"
        elif self.overall >= 85:
            return "B+"
        elif self.overall >= 80:
            return "B"
        elif self.overall >= 70:
            return "C"
        elif self.overall >= 60:
            return "D"
        else:
            return "F"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "table_name": self.table_name,
            "overall_score": self.overall,
            "grade": self.grade,
            "completeness_score": self.completeness,
            "accuracy_score": self.accuracy,
            "consistency_score": self.consistency,
            "timeliness_score": self.timeliness,
            "row_count": self.row_count,
            "failed_checks": self.failed_checks,
            "scored_at": self.scored_at,
            "details": self.details,
        }


class QualityScorer:
    """
    Computes quality scores from validation and anomaly results.

    Usage:
        scorer = QualityScorer()
        score = scorer.score(
            table_name="exercises",
            validation_results=validation_results,
            anomaly_results=anomaly_results,
            null_percentage=5.2,
            row_count=60,
        )
        print(f"Quality: {score.overall}/100 ({score.grade})")
    """

    # Weight of each dimension in the overall score
    WEIGHTS = {
        "completeness": 0.30,
        "accuracy": 0.30,
        "consistency": 0.20,
        "timeliness": 0.20,
    }

    def score(
        self,
        table_name: str,
        validation_results: list[ValidationResult] | None = None,
        anomaly_results: list[AnomalyResult] | None = None,
        null_percentage: float = 0.0,
        row_count: int = 0,
    ) -> QualityScore:
        """
        Compute a quality score for a table.

        Args:
            table_name: Name of the table being scored
            validation_results: Results from the validator
            anomaly_results: Results from the anomaly detector
            null_percentage: Overall null percentage from profiler
            row_count: Total row count

        Returns:
            QualityScore with overall and dimension scores
        """
        validation_results = validation_results or []
        anomaly_results = anomaly_results or []

        # Calculate each dimension
        completeness = self._score_completeness(validation_results, null_percentage)
        accuracy = self._score_accuracy(validation_results, anomaly_results)
        consistency = self._score_consistency(validation_results)
        timeliness = self._score_timeliness(validation_results)

        # Weighted overall score
        overall = (
            completeness * self.WEIGHTS["completeness"]
            + accuracy * self.WEIGHTS["accuracy"]
            + consistency * self.WEIGHTS["consistency"]
            + timeliness * self.WEIGHTS["timeliness"]
        )

        failed_checks = sum(1 for r in validation_results if not r.passed)

        score = QualityScore(
            table_name=table_name,
            overall=overall,
            completeness=completeness,
            accuracy=accuracy,
            consistency=consistency,
            timeliness=timeliness,
            row_count=row_count,
            failed_checks=failed_checks,
            details={
                "total_checks": len(validation_results),
                "passed_checks": len(validation_results) - failed_checks,
                "anomaly_checks": len(anomaly_results),
                "anomalies_found": sum(r.anomaly_count for r in anomaly_results),
            },
        )

        logger.info(
            f"Quality score for {table_name}: "
            f"{score.overall}/100 ({score.grade}) "
            f"[C={completeness:.0f} A={accuracy:.0f} "
            f"Co={consistency:.0f} T={timeliness:.0f}]",
            extra={"layer": "quality"},
        )

        return score

    @staticmethod
    def _score_completeness(
        results: list[ValidationResult],
        null_percentage: float,
    ) -> float:
        """
        Score based on data completeness (are required fields populated?).

        Factors:
          - Required column presence (from schema validation)
          - Overall null percentage
        """
        # Start at 100, deduct for issues
        score = 100.0

        # Deduct for missing required columns (critical)
        schema_results = [
            r for r in results if r.rule_name.startswith("schema_required_column")
        ]
        if schema_results:
            missing = sum(1 for r in schema_results if not r.passed)
            if missing > 0:
                score -= min(missing * 15, 60)  # Max 60 point deduction

        # Deduct for null percentage (proportional)
        # 0% nulls = no deduction, 50% nulls = 40 point deduction
        null_deduction = min(null_percentage * 0.8, 40)
        score -= null_deduction

        return max(score, 0.0)

    @staticmethod
    def _score_accuracy(
        results: list[ValidationResult],
        anomaly_results: list[AnomalyResult],
    ) -> float:
        """
        Score based on data accuracy (are values correct?).

        Factors:
          - Business rule pass rate
          - Anomaly percentage
        """
        score = 100.0

        # Business rule failures
        business_results = [
            r for r in results
            if not r.rule_name.startswith("schema_") and not r.rule_name.startswith("freshness_")
        ]

        if business_results:
            failed = sum(1 for r in business_results if not r.passed)
            total = len(business_results)
            pass_rate = (total - failed) / total if total > 0 else 1.0
            score = score * pass_rate

        # Anomaly deduction
        if anomaly_results:
            total_anomalies = sum(r.anomaly_count for r in anomaly_results)
            total_checked = sum(r.total_count for r in anomaly_results)
            if total_checked > 0:
                anomaly_rate = total_anomalies / total_checked
                score -= min(anomaly_rate * 100, 30)  # Max 30 point deduction

        return max(score, 0.0)

    @staticmethod
    def _score_consistency(results: list[ValidationResult]) -> float:
        """
        Score based on data consistency (are formats standardized?).

        Factors:
          - Type validation pass rate
          - In-set validation pass rate (enum consistency)
        """
        score = 100.0

        type_results = [r for r in results if r.rule_name.startswith("schema_type")]
        set_results = [r for r in results if "in_set" in r.rule_name or "valid_" in r.rule_name]

        all_consistency = type_results + set_results
        if all_consistency:
            failed = sum(1 for r in all_consistency if not r.passed)
            total = len(all_consistency)
            pass_rate = (total - failed) / total if total > 0 else 1.0
            score = score * pass_rate

        return max(score, 0.0)

    @staticmethod
    def _score_timeliness(results: list[ValidationResult]) -> float:
        """
        Score based on data timeliness (is the data fresh?).

        Factors:
          - Freshness check results
        """
        freshness_results = [r for r in results if r.rule_name.startswith("freshness_")]

        if not freshness_results:
            return 100.0  # No freshness checks configured — assume timely

        passed = all(r.passed for r in freshness_results)
        if passed:
            return 100.0

        # Partial score based on how stale
        for r in freshness_results:
            age_hours = r.details.get("age_hours", 0)
            if age_hours > 168:  # More than a week
                return 30.0
            elif age_hours > 72:  # More than 3 days
                return 60.0
            else:
                return 80.0

        return 50.0
