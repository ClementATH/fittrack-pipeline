"""
Anomaly Detector
=================

WHAT: Detects statistical anomalies in data using Z-score and IQR methods.
Flags values that deviate significantly from expected distributions.

WHY: Even if data passes schema and business rule validation, it can still
be wrong. A body weight of 85 kg is valid, but if yesterday it was 80 kg,
that 5 kg jump in one day is suspicious. Anomaly detection catches these
"valid but unlikely" values.

# LEARN: There are two primary statistical methods for anomaly detection:
#
# 1. Z-Score Method:
#    How many standard deviations from the mean?
#    z = (value - mean) / std_dev
#    If |z| > 3, the value is an outlier (99.7% rule)
#
# 2. IQR Method (Interquartile Range):
#    Q1 = 25th percentile, Q3 = 75th percentile
#    IQR = Q3 - Q1
#    Outlier if value < Q1 - 1.5*IQR or value > Q3 + 1.5*IQR
#
# Z-Score works well for normally distributed data.
# IQR is more robust and works for skewed distributions.
#
# At WellMed, these same methods detect anomalies in patient vitals,
# lab results, and claim amounts.
"""

from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.anomaly")


class AnomalyResult:
    """Container for anomaly detection results on a single column."""

    def __init__(
        self,
        column: str,
        method: str,
        anomaly_count: int,
        total_count: int,
        threshold: float,
        anomaly_indices: list[int] | None = None,
        stats: dict[str, float] | None = None,
    ):
        self.column = column
        self.method = method
        self.anomaly_count = anomaly_count
        self.total_count = total_count
        self.anomaly_pct = round(anomaly_count / total_count * 100, 2) if total_count > 0 else 0
        self.threshold = threshold
        self.anomaly_indices = anomaly_indices or []
        self.stats = stats or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "method": self.method,
            "anomaly_count": self.anomaly_count,
            "total_count": self.total_count,
            "anomaly_pct": self.anomaly_pct,
            "threshold": self.threshold,
            "stats": self.stats,
        }


class AnomalyDetector:
    """
    Statistical anomaly detection for numeric data.

    Usage:
        detector = AnomalyDetector(z_threshold=3.0, iqr_multiplier=1.5)
        results = detector.detect(df, columns=["weight_kg", "calories"])
    """

    def __init__(
        self,
        z_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        min_sample_size: int = 10,
    ):
        """
        Args:
            z_threshold: Z-score threshold for flagging outliers (default 3.0)
            iqr_multiplier: IQR fence multiplier (default 1.5)
            min_sample_size: Minimum rows needed before detection runs
        """
        self.z_threshold = z_threshold
        self.iqr_multiplier = iqr_multiplier
        self.min_sample_size = min_sample_size

    def detect(
        self,
        df: pd.DataFrame,
        columns: list[str] | None = None,
    ) -> list[AnomalyResult]:
        """
        Run anomaly detection on specified columns.

        Args:
            df: DataFrame to analyze
            columns: Columns to check. If None, checks all numeric columns.

        Returns:
            List of AnomalyResult objects
        """
        if columns is None:
            columns = df.select_dtypes(include=["number"]).columns.tolist()

        results: list[AnomalyResult] = []

        for col in columns:
            if col not in df.columns:
                continue

            series = pd.to_numeric(df[col], errors="coerce").dropna()

            if len(series) < self.min_sample_size:
                logger.debug(
                    f"Skipping anomaly detection for {col}: "
                    f"only {len(series)} non-null values "
                    f"(min: {self.min_sample_size})"
                )
                continue

            # Run both methods
            z_result = self._detect_zscore(series, col)
            iqr_result = self._detect_iqr(series, col)

            results.extend([z_result, iqr_result])

        anomaly_count = sum(r.anomaly_count for r in results)
        if anomaly_count > 0:
            logger.warning(
                f"Anomaly detection found {anomaly_count} anomalies "
                f"across {len(columns)} columns",
                extra={"layer": "quality"},
            )

        return results

    def _detect_zscore(self, series: pd.Series, col: str) -> AnomalyResult:
        """
        Detect outliers using Z-score method.

        # LEARN: Z-Score = (value - mean) / standard_deviation
        # A z-score of 3 means the value is 3 standard deviations
        # from the mean. For normal distributions, only 0.3% of values
        # should be beyond +-3 std devs. If you see more, something's off.
        """
        mean = series.mean()
        std = series.std()

        if std == 0:
            return AnomalyResult(
                column=col,
                method="z_score",
                anomaly_count=0,
                total_count=len(series),
                threshold=self.z_threshold,
                stats={"mean": mean, "std": 0},
            )

        z_scores = ((series - mean) / std).abs()
        anomalies = z_scores > self.z_threshold
        anomaly_indices = series.index[anomalies].tolist()

        return AnomalyResult(
            column=col,
            method="z_score",
            anomaly_count=int(anomalies.sum()),
            total_count=len(series),
            threshold=self.z_threshold,
            anomaly_indices=anomaly_indices,
            stats={
                "mean": round(float(mean), 4),
                "std": round(float(std), 4),
                "max_z_score": round(float(z_scores.max()), 4),
            },
        )

    def _detect_iqr(self, series: pd.Series, col: str) -> AnomalyResult:
        """
        Detect outliers using Interquartile Range (IQR) method.

        # LEARN: IQR is more robust than Z-score for skewed distributions.
        # It doesn't assume normality, making it better for real-world data
        # like income, workout volume, or calorie counts which are often
        # right-skewed (most values are low, some are very high).
        #
        # IQR = Q3 - Q1 (the range of the middle 50% of data)
        # Lower fence = Q1 - 1.5 * IQR
        # Upper fence = Q3 + 1.5 * IQR
        # Anything outside the fences is an outlier.
        """
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        if iqr == 0:
            return AnomalyResult(
                column=col,
                method="iqr",
                anomaly_count=0,
                total_count=len(series),
                threshold=self.iqr_multiplier,
                stats={"q1": float(q1), "q3": float(q3), "iqr": 0},
            )

        lower_fence = q1 - self.iqr_multiplier * iqr
        upper_fence = q3 + self.iqr_multiplier * iqr

        anomalies = (series < lower_fence) | (series > upper_fence)
        anomaly_indices = series.index[anomalies].tolist()

        return AnomalyResult(
            column=col,
            method="iqr",
            anomaly_count=int(anomalies.sum()),
            total_count=len(series),
            threshold=self.iqr_multiplier,
            anomaly_indices=anomaly_indices,
            stats={
                "q1": round(float(q1), 4),
                "q3": round(float(q3), 4),
                "iqr": round(float(iqr), 4),
                "lower_fence": round(float(lower_fence), 4),
                "upper_fence": round(float(upper_fence), 4),
            },
        )
