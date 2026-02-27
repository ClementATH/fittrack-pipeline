"""
Quality Report Generator
=========================

WHAT: Generates human-readable Markdown quality reports from profiling,
validation, and scoring results.

WHY: Quality scores are great for monitoring, but when something fails,
engineers need detailed reports to debug. Reports combine:
  - Table profiles (what does the data look like?)
  - Validation results (what rules passed/failed?)
  - Anomaly findings (what's statistically unusual?)
  - Quality scores (what's the overall grade?)

# LEARN: Reports serve two audiences:
#   1. Engineers: Need detailed column stats and failure reasons for debugging
#   2. Stakeholders: Need the overall score and summary
# A good report starts with the summary (for stakeholders) and drills
# down into details (for engineers). This is the "inverted pyramid"
# style from journalism — most important info first.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.quality.scorer import QualityScore
from src.quality.validator import ValidationResult
from src.quality.anomaly_detector import AnomalyResult
from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.reporter")


class QualityReporter:
    """
    Generates Markdown quality reports.

    Usage:
        reporter = QualityReporter(reports_dir="reports")
        path = reporter.generate_report(
            table_name="exercises",
            quality_score=score,
            validation_results=results,
            anomaly_results=anomalies,
            profile=profile,
        )
    """

    def __init__(self, reports_dir: str = "reports"):
        self.reports_dir = Path(reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def generate_report(
        self,
        table_name: str,
        quality_score: QualityScore,
        validation_results: list[ValidationResult] | None = None,
        anomaly_results: list[AnomalyResult] | None = None,
        profile: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> Path:
        """
        Generate a comprehensive quality report as Markdown.

        Returns:
            Path to the generated report file
        """
        validation_results = validation_results or []
        anomaly_results = anomaly_results or []

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"quality_report_{table_name}_{timestamp}.md"
        report_path = self.reports_dir / filename

        sections: list[str] = []

        # Header
        sections.append(self._header(table_name, quality_score, run_id))

        # Score Summary
        sections.append(self._score_summary(quality_score))

        # Validation Results
        if validation_results:
            sections.append(self._validation_section(validation_results))

        # Anomaly Results
        if anomaly_results:
            sections.append(self._anomaly_section(anomaly_results))

        # Profile Summary
        if profile:
            sections.append(self._profile_section(profile))

        # Footer
        sections.append(self._footer())

        report_content = "\n\n".join(sections)
        report_path.write_text(report_content, encoding="utf-8")

        logger.info(
            f"Quality report generated: {report_path}",
            extra={"layer": "quality"},
        )
        return report_path

    @staticmethod
    def _header(
        table_name: str,
        score: QualityScore,
        run_id: str | None,
    ) -> str:
        """Generate report header with key metrics."""
        grade_emoji = {
            "A+": "[A+]", "A": "[A]", "B+": "[B+]", "B": "[B]",
            "C": "[C]", "D": "[D]", "F": "[F]",
        }
        emoji = grade_emoji.get(score.grade, "[-]")

        lines = [
            f"# Data Quality Report: {table_name}",
            "",
            f"**Overall Score: {score.overall}/100 {emoji} Grade: {score.grade}**",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Table | {table_name} |",
            f"| Rows | {score.row_count:,} |",
            f"| Score | {score.overall}/100 |",
            f"| Grade | {score.grade} |",
            f"| Failed Checks | {score.failed_checks} |",
            f"| Scored At | {score.scored_at} |",
        ]
        if run_id:
            lines.append(f"| Run ID | {run_id} |")

        return "\n".join(lines)

    @staticmethod
    def _score_summary(score: QualityScore) -> str:
        """Generate score breakdown section."""
        def bar(value: float, width: int = 20) -> str:
            filled = int(value / 100 * width)
            return "[" + "#" * filled + "." * (width - filled) + "]"

        lines = [
            "## Quality Dimensions",
            "",
            "| Dimension | Score | Visual |",
            "|-----------|-------|--------|",
            f"| Completeness | {score.completeness:.1f} | {bar(score.completeness)} |",
            f"| Accuracy | {score.accuracy:.1f} | {bar(score.accuracy)} |",
            f"| Consistency | {score.consistency:.1f} | {bar(score.consistency)} |",
            f"| Timeliness | {score.timeliness:.1f} | {bar(score.timeliness)} |",
            f"| **Overall** | **{score.overall:.1f}** | **{bar(score.overall)}** |",
        ]
        return "\n".join(lines)

    @staticmethod
    def _validation_section(results: list[ValidationResult]) -> str:
        """Generate validation results section."""
        passed = [r for r in results if r.passed]
        failed = [r for r in results if not r.passed]

        lines = [
            "## Validation Results",
            "",
            f"**{len(passed)} passed, {len(failed)} failed "
            f"out of {len(results)} checks**",
            "",
        ]

        if failed:
            lines.append("### Failed Checks")
            lines.append("")
            lines.append("| Rule | Severity | Column | Message | Failing Rows |")
            lines.append("|------|----------|--------|---------|--------------|")
            for r in sorted(failed, key=lambda x: {"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(x.severity, 3)):
                lines.append(
                    f"| {r.rule_name} | {r.severity} | {r.column or '-'} | "
                    f"{r.message} | {r.failing_rows} |"
                )
            lines.append("")

        if passed:
            lines.append("### Passed Checks")
            lines.append("")
            lines.append("| Rule | Column | Message |")
            lines.append("|------|--------|---------|")
            for r in passed:
                lines.append(f"| {r.rule_name} | {r.column or '-'} | {r.message} |")

        return "\n".join(lines)

    @staticmethod
    def _anomaly_section(results: list[AnomalyResult]) -> str:
        """Generate anomaly detection section."""
        anomalies_found = [r for r in results if r.anomaly_count > 0]

        lines = [
            "## Anomaly Detection",
            "",
            f"**{len(anomalies_found)} columns with anomalies detected**",
            "",
            "| Column | Method | Anomalies | Total | Pct | Threshold |",
            "|--------|--------|-----------|-------|-----|-----------|",
        ]
        for r in results:
            flag = " [!]" if r.anomaly_count > 0 else ""
            lines.append(
                f"| {r.column} | {r.method} | {r.anomaly_count}{flag} | "
                f"{r.total_count} | {r.anomaly_pct}% | {r.threshold} |"
            )

        return "\n".join(lines)

    @staticmethod
    def _profile_section(profile: dict[str, Any]) -> str:
        """Generate data profile section."""
        summary = profile.get("summary", {})
        lines = [
            "## Data Profile Summary",
            "",
            f"- **Rows:** {summary.get('row_count', 0):,}",
            f"- **Columns:** {summary.get('column_count', 0)}",
            f"- **Memory:** {summary.get('memory_usage_mb', 0)} MB",
            f"- **Duplicate Rows:** {summary.get('duplicate_rows', 0):,}",
            f"- **Total Null Cells:** {summary.get('total_null_cells', 0):,}",
            f"- **Null Percentage:** {summary.get('null_percentage', 0)}%",
        ]

        warnings = profile.get("warnings", [])
        if warnings:
            lines.append("")
            lines.append("### Profile Warnings")
            for w in warnings:
                lines.append(f"- [{w['severity']}] **{w['column']}**: {w['warning']}")

        return "\n".join(lines)

    @staticmethod
    def _footer() -> str:
        """Generate report footer."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return (
            "---\n"
            f"*Generated by FitTrack Pro ETL Pipeline — {now}*\n"
            "*Data quality scores follow the DAMA-DMBOK framework*"
        )
