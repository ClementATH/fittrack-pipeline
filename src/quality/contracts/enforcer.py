"""
Contract Enforcer
==================

Validates DataFrames against Pydantic data contracts at the row level.
Returns structured results that integrate with the existing quality
pipeline (profiler -> validator -> anomaly -> scorer).

# LEARN: Row-level validation catches issues that aggregate checks miss.
# A schema check says "this column exists and is numeric." A contract
# check says "every row in this column is between 30 and 300." The
# combination gives you defense in depth.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.quality.contracts.models import (
    BodyMetricContract,
    ExerciseContract,
    NutritionLogContract,
    WorkoutContract,
)
from src.utils.logger import get_logger

logger = get_logger("fittrack.quality.contracts")


# ============================================================
# Registry: maps table names to their contract models
# ============================================================
CONTRACT_REGISTRY: dict[str, type[BaseModel]] = {
    "exercises": ExerciseContract,
    "workouts": WorkoutContract,
    "body_metrics": BodyMetricContract,
    "nutrition_logs": NutritionLogContract,
}


# ============================================================
# Result objects
# ============================================================
class ContractViolation:
    """A single row-level contract violation."""

    def __init__(self, row_index: int, field: str, message: str, value: Any = None):
        self.row_index = row_index
        self.field = field
        self.message = message
        self.value = value

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_index": self.row_index,
            "field": self.field,
            "message": self.message,
            "value": str(self.value),
        }


class ContractResult:
    """Aggregated result of enforcing a contract against a DataFrame."""

    def __init__(
        self,
        table_name: str,
        total_rows: int,
        valid_rows: int,
        violations: list[ContractViolation],
    ):
        self.table_name = table_name
        self.total_rows = total_rows
        self.valid_rows = valid_rows
        self.violations = violations
        self.passed = len(violations) == 0

    @property
    def violation_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.total_rows - self.valid_rows) / self.total_rows * 100

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "violation_count": len(self.violations),
            "violation_rate_pct": round(self.violation_rate, 2),
            "passed": self.passed,
            "violations": [v.to_dict() for v in self.violations[:20]],
        }


# ============================================================
# Enforcer
# ============================================================
class ContractEnforcer:
    """Validates DataFrames against registered Pydantic data contracts."""

    def __init__(self) -> None:
        self.contracts = dict(CONTRACT_REGISTRY)

    def enforce(
        self,
        df: pd.DataFrame,
        table_name: str,
        sample_size: int | None = None,
    ) -> ContractResult:
        """
        Validate a DataFrame against its registered contract.

        Args:
            df: DataFrame to validate
            table_name: Which contract to apply (must be in CONTRACT_REGISTRY)
            sample_size: If set, only validate a random sample of rows

        Returns:
            ContractResult with violations (if any)

        Raises:
            KeyError: If table_name has no registered contract
        """
        if table_name not in self.contracts:
            msg = f"No contract registered for table '{table_name}'"
            raise KeyError(msg)

        model = self.contracts[table_name]
        check_df = df.sample(n=sample_size) if sample_size and sample_size < len(df) else df

        violations: list[ContractViolation] = []
        valid_count = 0

        for idx, row in check_df.iterrows():
            row_dict = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            try:
                model.model_validate(row_dict)
                valid_count += 1
            except ValidationError as e:
                for error in e.errors():
                    field = ".".join(str(loc) for loc in error["loc"])
                    violations.append(
                        ContractViolation(
                            row_index=int(idx),  # type: ignore[arg-type]
                            field=field,
                            message=error["msg"],
                            value=row_dict.get(field),
                        )
                    )

        result = ContractResult(
            table_name=table_name,
            total_rows=len(check_df),
            valid_rows=valid_count,
            violations=violations,
        )

        if result.passed:
            logger.info(
                f"Contract check PASSED for {table_name}: {valid_count}/{len(check_df)} rows valid",
                extra={"layer": "quality"},
            )
        else:
            logger.warning(
                f"Contract check found {len(violations)} violations in {table_name}: "
                f"{valid_count}/{len(check_df)} rows valid "
                f"({result.violation_rate:.1f}% violation rate)",
                extra={"layer": "quality"},
            )

        return result

    @staticmethod
    def generate_json_schemas(
        output_dir: str = "src/quality/contracts/schemas",
    ) -> dict[str, str]:
        """
        Generate JSON Schema files from all registered contract models.

        Returns:
            dict mapping table_name -> file path of generated schema
        """
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        generated: dict[str, str] = {}
        for table_name, model in CONTRACT_REGISTRY.items():
            schema = model.model_json_schema()
            file_path = out_path / f"{table_name}.schema.json"
            file_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
            generated[table_name] = str(file_path)
            logger.info(f"Generated JSON Schema: {file_path}")

        return generated
