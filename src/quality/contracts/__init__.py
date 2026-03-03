"""
Data Contract Enforcement
==========================

WHAT: Pydantic models defining the expected schema for each dataset at the
Silver layer. A ContractEnforcer validates DataFrames row-by-row against
these contracts and reports violations.

WHY: Data contracts make implicit assumptions explicit. When an upstream
API changes its response format or a CSV column gets renamed, the contract
catches it immediately instead of letting bad data silently corrupt Gold.

# LEARN: Data contracts are a hot topic in modern data engineering.
# Tools like dbt contracts, Soda, and Great Expectations implement similar
# concepts. The idea is simple: treat your data interfaces like API
# contracts — if the schema changes, the pipeline should break loudly.
"""

from src.quality.contracts.enforcer import ContractEnforcer, ContractResult, ContractViolation
from src.quality.contracts.models import (
    BodyMetricContract,
    ExerciseContract,
    NutritionLogContract,
    WorkoutContract,
)

__all__ = [
    "BodyMetricContract",
    "ContractEnforcer",
    "ContractResult",
    "ContractViolation",
    "ExerciseContract",
    "NutritionLogContract",
    "WorkoutContract",
]
