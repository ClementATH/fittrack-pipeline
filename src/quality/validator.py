"""
Rule-Based Data Validator
==========================

WHAT: Validates data against YAML-defined rules (schema rules, business rules,
freshness rules). Returns detailed validation results with pass/fail per rule.

WHY: Validation is your safety net. It catches bad data BEFORE it enters
the Gold layer where analysts and dashboards consume it. Without validation:
  - Invalid data corrupts reports and dashboards
  - Bad data silently propagates and compounds
  - You discover issues weeks later when a stakeholder complains

# LEARN: The validation engine is YAML-driven by design. This means:
#   - Domain experts can add rules without writing Python
#   - Rules are version-controlled alongside your code
#   - You can have different rule sets for different environments
#   - Adding a new check is a config change, not a code change
#
# At WellMed, think about this: if a nurse enters "999" for blood pressure,
# your validation rules should catch that before it hits the analytics layer.
"""

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.validator")


class ValidationResult:
    """Container for a single validation check result."""

    def __init__(
        self,
        rule_name: str,
        table_name: str,
        passed: bool,
        severity: str = "INFO",
        message: str = "",
        failing_rows: int = 0,
        total_rows: int = 0,
        column: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.rule_name = rule_name
        self.table_name = table_name
        self.passed = passed
        self.severity = severity
        self.message = message
        self.failing_rows = failing_rows
        self.total_rows = total_rows
        self.column = column
        self.details = details or {}
        self.checked_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "table_name": self.table_name,
            "passed": self.passed,
            "severity": self.severity,
            "message": self.message,
            "failing_rows": self.failing_rows,
            "total_rows": self.total_rows,
            "column": self.column,
            "checked_at": self.checked_at,
            "details": self.details,
        }


class DataValidator:
    """
    YAML-driven data validation engine.

    Validates DataFrames against rules defined in config/quality_rules.yaml.
    Returns structured results that feed into the quality scorer and reporter.

    Usage:
        validator = DataValidator(quality_rules)
        results = validator.validate(df, table_name="exercises")
        for r in results:
            print(f"{r.rule_name}: {'PASS' if r.passed else 'FAIL'}")
    """

    def __init__(self, quality_rules: dict[str, Any]):
        self.schema_rules = quality_rules.get("schema_rules", {})
        self.business_rules = quality_rules.get("business_rules", {})
        self.freshness_rules = quality_rules.get("freshness_rules", {})

    def validate(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> list[ValidationResult]:
        """
        Run all applicable validation rules against a DataFrame.

        Args:
            df: DataFrame to validate
            table_name: Which table's rules to apply

        Returns:
            List of ValidationResult objects
        """
        results: list[ValidationResult] = []

        logger.info(
            f"Validating {table_name}: {len(df)} rows",
            extra={"layer": "quality"},
        )

        # Schema validation
        if table_name in self.schema_rules:
            results.extend(self._validate_schema(df, table_name))

        # Business rule validation
        if table_name in self.business_rules:
            results.extend(self._validate_business_rules(df, table_name))

        # Freshness validation
        if table_name in self.freshness_rules:
            results.extend(self._validate_freshness(df, table_name))

        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
        logger.info(
            f"Validation complete for {table_name}: "
            f"{passed} passed, {failed} failed out of {len(results)} checks",
            extra={"layer": "quality"},
        )

        return results

    def _validate_schema(
        self, df: pd.DataFrame, table_name: str
    ) -> list[ValidationResult]:
        """
        Validate that required columns exist and have correct types.

        # LEARN: Schema validation is your first line of defense.
        # If the API changes its response format (adds/removes/renames columns),
        # schema validation catches it before any transformation code runs.
        """
        results: list[ValidationResult] = []
        schema = self.schema_rules[table_name]
        severity = schema.get("severity", "CRITICAL")

        # Check required columns
        required = schema.get("required_columns", [])
        for col in required:
            present = col in df.columns
            results.append(ValidationResult(
                rule_name=f"schema_required_column_{col}",
                table_name=table_name,
                passed=present,
                severity=severity,
                message=f"Required column '{col}' {'found' if present else 'MISSING'}",
                column=col,
            ))

        # Check column types
        expected_types = schema.get("column_types", {})
        for col, expected_type in expected_types.items():
            if col not in df.columns:
                continue

            actual_type = str(df[col].dtype)
            type_matches = self._check_type_compatibility(actual_type, expected_type)
            results.append(ValidationResult(
                rule_name=f"schema_type_{col}",
                table_name=table_name,
                passed=type_matches,
                severity="WARNING",
                message=(
                    f"Column '{col}' type: expected {expected_type}, "
                    f"got {actual_type}"
                ),
                column=col,
                details={"expected_type": expected_type, "actual_type": actual_type},
            ))

        return results

    def _validate_business_rules(
        self, df: pd.DataFrame, table_name: str
    ) -> list[ValidationResult]:
        """
        Validate domain-specific business rules.

        Each rule in the YAML config specifies:
          - column: Which column to check
          - check: What type of check (in_set, range, not_empty, min, max)
          - severity: How bad is a failure
        """
        results: list[ValidationResult] = []
        rules = self.business_rules[table_name]

        for rule in rules:
            rule_name = rule.get("rule", "unnamed")
            column = rule.get("column")
            check_type = rule.get("check")
            severity = rule.get("severity", "WARNING")

            if column not in df.columns:
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found for rule '{rule_name}'",
                    column=column,
                ))
                continue

            series = df[column].dropna()

            if check_type == "not_empty":
                failing = series.astype(str).str.strip().eq("").sum()
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=int(failing) == 0,
                    severity=severity,
                    message=rule.get("description", f"{column} not empty check"),
                    failing_rows=int(failing),
                    total_rows=len(series),
                    column=column,
                ))

            elif check_type == "in_set":
                allowed = set(rule.get("allowed_values", []))
                invalid = ~series.astype(str).isin(allowed)
                failing = int(invalid.sum())
                invalid_values = series[invalid].unique()[:5].tolist() if failing > 0 else []
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=failing == 0,
                    severity=severity,
                    message=rule.get("description", f"{column} in-set check"),
                    failing_rows=failing,
                    total_rows=len(series),
                    column=column,
                    details={"invalid_values": invalid_values},
                ))

            elif check_type == "range":
                min_val = rule.get("min")
                max_val = rule.get("max")
                numeric = pd.to_numeric(series, errors="coerce")
                out_of_range = ((numeric < min_val) | (numeric > max_val)) if (min_val is not None and max_val is not None) else pd.Series([False])
                failing = int(out_of_range.sum())
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=failing == 0,
                    severity=severity,
                    message=rule.get("description", f"{column} range check [{min_val}, {max_val}]"),
                    failing_rows=failing,
                    total_rows=len(series),
                    column=column,
                    details={"min": min_val, "max": max_val},
                ))

            elif check_type == "min":
                min_val = rule.get("min")
                numeric = pd.to_numeric(series, errors="coerce")
                below_min = numeric < min_val
                failing = int(below_min.sum())
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=failing == 0,
                    severity=severity,
                    message=rule.get("description", f"{column} >= {min_val}"),
                    failing_rows=failing,
                    total_rows=len(series),
                    column=column,
                ))

            elif check_type == "max":
                max_val = rule.get("max")
                numeric = pd.to_numeric(series, errors="coerce")
                above_max = numeric > max_val
                failing = int(above_max.sum())
                results.append(ValidationResult(
                    rule_name=rule_name,
                    table_name=table_name,
                    passed=failing == 0,
                    severity=severity,
                    message=rule.get("description", f"{column} <= {max_val}"),
                    failing_rows=failing,
                    total_rows=len(series),
                    column=column,
                ))

        return results

    def _validate_freshness(
        self, df: pd.DataFrame, table_name: str
    ) -> list[ValidationResult]:
        """
        Check if the data is recent enough based on freshness SLAs.

        # LEARN: Freshness checks catch stale data. If your pipeline
        # runs daily but the source data hasn't updated in 3 days,
        # something is wrong. At WellMed, if patient data is more than
        # 48 hours old, downstream clinical systems might make decisions
        # on outdated information — that's a patient safety issue.
        """
        rule = self.freshness_rules[table_name]
        max_age_hours = rule.get("max_age_hours", 48)
        severity = rule.get("severity", "WARNING")

        # Try to find a date column
        date_cols = [c for c in df.columns if any(
            d in c.lower() for d in ["date", "timestamp", "at", "time"]
        )]

        if not date_cols:
            return [ValidationResult(
                rule_name=f"freshness_{table_name}",
                table_name=table_name,
                passed=True,
                severity="INFO",
                message="No date column found for freshness check",
            )]

        date_col = date_cols[0]
        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()

        if dates.empty:
            return [ValidationResult(
                rule_name=f"freshness_{table_name}",
                table_name=table_name,
                passed=False,
                severity=severity,
                message="No valid dates found for freshness check",
            )]

        max_date = dates.max()
        now = pd.Timestamp.now(tz="UTC")
        if max_date.tzinfo is None:
            max_date = max_date.tz_localize("UTC")

        age_hours = (now - max_date).total_seconds() / 3600

        return [ValidationResult(
            rule_name=f"freshness_{table_name}",
            table_name=table_name,
            passed=age_hours <= max_age_hours,
            severity=severity,
            message=(
                f"Data age: {age_hours:.1f} hours "
                f"(max allowed: {max_age_hours} hours)"
            ),
            details={"max_date": str(max_date), "age_hours": round(age_hours, 1)},
        )]

    @staticmethod
    def _check_type_compatibility(actual: str, expected: str) -> bool:
        """Check if actual pandas dtype is compatible with expected type."""
        type_map: dict[str, set[str]] = {
            "string": {"object", "string", "str"},
            "integer": {"int64", "Int64", "int32", "Int32"},
            "numeric": {"float64", "float32", "int64", "Int64"},
            "date": {"datetime64[ns]", "object", "datetime64[ns, UTC]"},
            "boolean": {"bool", "boolean"},
        }
        allowed = type_map.get(expected, {expected})
        return actual in allowed
