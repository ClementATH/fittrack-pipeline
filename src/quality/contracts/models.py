"""
Data Contract Models
=====================

Pydantic models representing the expected Silver-layer schema for each
dataset. These are the single source of truth for what "valid data" looks
like after cleaning and transformation.

Each model maps directly to the schema and business rules defined in
config/quality_rules.yaml, but enforced at the row level with rich error
messages.

# LEARN: Using Pydantic for data contracts gives you:
#   1. Type validation (string, int, float) for free
#   2. Range/enum validation via Field() and validators
#   3. Auto-generated JSON Schema via model_json_schema()
#   4. Clear error messages pointing to the exact field that failed
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

# ============================================================
# Shared constants (matching quality_rules.yaml)
# ============================================================
VALID_MUSCLE_GROUPS = frozenset(
    {
        "chest",
        "back",
        "shoulders",
        "biceps",
        "triceps",
        "forearms",
        "quads",
        "hamstrings",
        "glutes",
        "calves",
        "abs",
        "obliques",
        "traps",
        "lats",
        "hip_flexors",
        "adductors",
        "abductors",
        "neck",
        "full_body",
    }
)

VALID_WORKOUT_STATUSES = frozenset(
    {
        "completed",
        "skipped",
        "partial",
        "planned",
    }
)

VALID_MEAL_TYPES = frozenset(
    {
        "breakfast",
        "lunch",
        "dinner",
        "pre_workout",
        "post_workout",
        "snack",
        "supplement",
    }
)


# ============================================================
# Contract models
# ============================================================
class ExerciseContract(BaseModel):
    """Contract for exercise data at the Silver layer."""

    name: str = Field(..., min_length=1, description="Exercise name, must not be empty")
    primary_muscle: str = Field(..., description="Must be a recognized muscle group")
    exercise_type: str = Field(default="compound")
    equipment: str = Field(default="barbell")
    difficulty: str = Field(default="intermediate")
    slug: str | None = None
    is_unilateral: bool | None = None

    @field_validator("primary_muscle")
    @classmethod
    def validate_muscle_group(cls, v: str) -> str:
        if v not in VALID_MUSCLE_GROUPS:
            msg = f"Invalid muscle group: {v}"
            raise ValueError(msg)
        return v


class WorkoutContract(BaseModel):
    """Contract for workout data at the Silver layer."""

    athlete_id: str = Field(..., min_length=1)
    workout_date: str = Field(..., description="Date string (various formats accepted)")
    status: str = Field(...)

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        if v not in VALID_WORKOUT_STATUSES:
            msg = f"Invalid status: {v}"
            raise ValueError(msg)
        return v


class BodyMetricContract(BaseModel):
    """Contract for body metrics at the Silver layer."""

    athlete_id: str | None = None
    measured_at: str | None = None
    weight_kg: float = Field(..., ge=30.0, le=300.0)
    body_fat_pct: float | None = Field(None, ge=3.0, le=60.0)
    resting_heart_rate: int | None = Field(None, ge=30, le=120)


class NutritionLogContract(BaseModel):
    """Contract for nutrition log data at the Silver layer."""

    athlete_id: str | None = None
    log_date: str = Field(...)
    meal_type: str = Field(...)
    calories: float = Field(..., ge=0, le=5000)
    protein_g: float | None = Field(None, ge=0)
    carbs_g: float | None = Field(None, ge=0)
    fats_g: float | None = Field(None, ge=0)

    @field_validator("meal_type")
    @classmethod
    def validate_meal_type(cls, v: str) -> str:
        if v not in VALID_MEAL_TYPES:
            msg = f"Invalid meal type: {v}"
            raise ValueError(msg)
        return v
