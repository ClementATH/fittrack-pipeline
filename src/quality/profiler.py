"""
Data Profiler
==============

WHAT: Generates statistical profiles of datasets — column stats, distributions,
cardinality, missing percentages, and unique counts.

WHY: Before you validate or transform data, you need to UNDERSTAND it.
Profiling answers:
  - How many nulls are in each column?
  - What's the distribution of values?
  - Are there unexpected outliers?
  - How many unique values does each column have?

# LEARN: Data profiling is the first thing a senior DE does when encountering
# a new data source. At WellMed, when a new vendor sends data, you should
# ALWAYS profile it before writing any transformation code. It prevents
# assumptions that lead to bugs.
"""

from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.profiler")


class DataProfiler:
    """
    Generates statistical profiles for DataFrames.

    Usage:
        profiler = DataProfiler()
        profile = profiler.profile(df, table_name="exercises")
        print(profile["summary"])
    """

    def profile(self, df: pd.DataFrame, table_name: str = "unknown") -> dict[str, Any]:
        """
        Generate a comprehensive profile for a DataFrame.

        Returns:
            Dictionary with:
            - summary: High-level table stats
            - columns: Per-column statistics
            - warnings: Potential data quality issues detected
        """
        logger.info(
            f"Profiling {table_name}: {len(df)} rows, {len(df.columns)} columns",
            extra={"layer": "quality"},
        )

        profile_result: dict[str, Any] = {
            "table_name": table_name,
            "summary": self._table_summary(df),
            "columns": {},
            "warnings": [],
        }

        for col in df.columns:
            col_profile = self._column_profile(df, col)
            profile_result["columns"][col] = col_profile

            # Check for warnings
            warnings = self._check_column_warnings(col, col_profile)
            profile_result["warnings"].extend(warnings)

        logger.info(
            f"Profiling complete for {table_name}: "
            f"{len(profile_result['warnings'])} warnings",
            extra={"layer": "quality"},
        )
        return profile_result

    @staticmethod
    def _table_summary(df: pd.DataFrame) -> dict[str, Any]:
        """Generate high-level table statistics."""
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "duplicate_rows": int(df.duplicated().sum()),
            "completely_empty_columns": int((df.isna().all()).sum()),
            "columns_with_nulls": int((df.isna().any()).sum()),
            "total_null_cells": int(df.isna().sum().sum()),
            "null_percentage": round(
                df.isna().sum().sum() / (len(df) * len(df.columns)) * 100, 2
            ) if len(df) > 0 and len(df.columns) > 0 else 0,
        }

    @staticmethod
    def _column_profile(df: pd.DataFrame, col: str) -> dict[str, Any]:
        """Generate statistics for a single column."""
        series = df[col]
        profile: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_pct": round(series.isna().mean() * 100, 2),
            "unique_count": int(series.nunique()),
            "unique_pct": round(series.nunique() / len(series) * 100, 2) if len(series) > 0 else 0,
        }

        non_null = series.dropna()

        if len(non_null) == 0:
            profile["all_null"] = True
            return profile

        # Type-specific statistics
        if pd.api.types.is_numeric_dtype(series):
            profile.update({
                "min": float(non_null.min()),
                "max": float(non_null.max()),
                "mean": round(float(non_null.mean()), 4),
                "median": round(float(non_null.median()), 4),
                "std": round(float(non_null.std()), 4) if len(non_null) > 1 else 0,
                "q25": round(float(non_null.quantile(0.25)), 4),
                "q75": round(float(non_null.quantile(0.75)), 4),
                "zeros": int((non_null == 0).sum()),
                "negatives": int((non_null < 0).sum()),
            })
        elif pd.api.types.is_string_dtype(series) or series.dtype == "object":
            str_series = non_null.astype(str)
            profile.update({
                "min_length": int(str_series.str.len().min()),
                "max_length": int(str_series.str.len().max()),
                "avg_length": round(float(str_series.str.len().mean()), 1),
                "empty_strings": int((str_series == "").sum()),
            })
            # Top values (most frequent)
            top_values = non_null.value_counts().head(5)
            profile["top_values"] = {
                str(k): int(v) for k, v in top_values.items()
            }
        elif pd.api.types.is_datetime64_any_dtype(series):
            profile.update({
                "min_date": str(non_null.min()),
                "max_date": str(non_null.max()),
                "date_range_days": (non_null.max() - non_null.min()).days,
            })
        elif pd.api.types.is_bool_dtype(series):
            profile.update({
                "true_count": int(non_null.sum()),
                "false_count": int((~non_null).sum()),
                "true_pct": round(float(non_null.mean() * 100), 2),
            })

        return profile

    @staticmethod
    def _check_column_warnings(col: str, profile: dict[str, Any]) -> list[dict[str, str]]:
        """Check a column profile for potential quality issues."""
        warnings: list[dict[str, str]] = []

        # High null percentage
        null_pct = profile.get("null_pct", 0)
        if null_pct > 50:
            warnings.append({
                "column": col,
                "warning": f"High null percentage: {null_pct}%",
                "severity": "WARNING",
            })
        elif null_pct == 100:
            warnings.append({
                "column": col,
                "warning": "Column is completely empty",
                "severity": "CRITICAL",
            })

        # Single value (no variation)
        if profile.get("unique_count", 0) == 1 and profile.get("null_pct", 0) < 100:
            warnings.append({
                "column": col,
                "warning": "Column has only one unique value (constant)",
                "severity": "INFO",
            })

        # High cardinality in string columns (possible ID column)
        unique_pct = profile.get("unique_pct", 0)
        if unique_pct > 95 and profile.get("dtype") == "object":
            warnings.append({
                "column": col,
                "warning": f"Very high cardinality ({unique_pct}%) — possible ID column",
                "severity": "INFO",
            })

        return warnings

    def profile_to_markdown(self, profile: dict[str, Any]) -> str:
        """Convert a profile to a readable Markdown report."""
        lines: list[str] = []
        summary = profile["summary"]

        lines.append(f"# Data Profile: {profile['table_name']}")
        lines.append("")
        lines.append("## Summary")
        lines.append(f"- **Rows:** {summary['row_count']:,}")
        lines.append(f"- **Columns:** {summary['column_count']}")
        lines.append(f"- **Memory:** {summary['memory_usage_mb']} MB")
        lines.append(f"- **Duplicate Rows:** {summary['duplicate_rows']:,}")
        lines.append(f"- **Null Cells:** {summary['total_null_cells']:,} ({summary['null_percentage']}%)")
        lines.append("")

        if profile["warnings"]:
            lines.append("## Warnings")
            for w in profile["warnings"]:
                icon = {"CRITICAL": "[!]", "WARNING": "[?]", "INFO": "[i]"}.get(
                    w["severity"], "[-]"
                )
                lines.append(f"- {icon} **{w['column']}**: {w['warning']}")
            lines.append("")

        lines.append("## Column Details")
        lines.append("")
        lines.append("| Column | Type | Nulls | Unique | Min | Max | Mean |")
        lines.append("|--------|------|-------|--------|-----|-----|------|")

        for col, stats in profile["columns"].items():
            null_str = f"{stats['null_count']} ({stats['null_pct']}%)"
            unique_str = f"{stats['unique_count']} ({stats['unique_pct']}%)"
            min_val = stats.get("min", stats.get("min_date", stats.get("min_length", "-")))
            max_val = stats.get("max", stats.get("max_date", stats.get("max_length", "-")))
            mean_val = stats.get("mean", stats.get("avg_length", "-"))
            lines.append(
                f"| {col} | {stats['dtype']} | {null_str} | "
                f"{unique_str} | {min_val} | {max_val} | {mean_val} |"
            )

        return "\n".join(lines)
