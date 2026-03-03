"""
Test Configuration (conftest.py)
=================================

Shared fixtures for all test modules. Provides sample DataFrames,
temporary directories, and mock configurations.

# LEARN: conftest.py is pytest's convention for shared test fixtures.
# Every fixture defined here is automatically available in all test files
# without needing imports. This is the "DRY" principle applied to testing.
"""

import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# ============================================================
# Ensure project root is on the Python path
# ============================================================

PROJECT_ROOT = Path(__file__).parent.parent
os.chdir(PROJECT_ROOT)


# ============================================================
# Sample DataFrames (used across all test modules)
# ============================================================


@pytest.fixture
def sample_exercises_df() -> pd.DataFrame:
    """Sample exercise data mimicking Wger API response after cleaning."""
    return pd.DataFrame(
        [
            {
                "id": 192,
                "name": "Barbell Bench Press",
                "description": "Flat bench press targeting chest.",
                "muscles": [4, 2, 5],
                "equipment": [1, 8],
                "category": 11,
            },
            {
                "id": 289,
                "name": "Barbell Deadlift",
                "description": "Conventional deadlift from the floor.",
                "muscles": [10, 11, 8, 15],
                "equipment": [1],
                "category": 10,
            },
            {
                "id": 111,
                "name": "Barbell Squat",
                "description": "High-bar back squat.",
                "muscles": [10, 8, 14],
                "equipment": [1, 8],
                "category": 9,
            },
            {
                "id": 274,
                "name": "Overhead Press",
                "description": "Standing barbell OHP.",
                "muscles": [2, 5, 13],
                "equipment": [1],
                "category": 13,
            },
            {
                "id": 106,
                "name": "Barbell Row",
                "description": "Bent-over barbell row.",
                "muscles": [12, 13, 3],
                "equipment": [1],
                "category": 12,
            },
        ]
    )


@pytest.fixture
def sample_workouts_df() -> pd.DataFrame:
    """Sample workout log data mimicking CSV file upload."""
    return pd.DataFrame(
        [
            {
                "date": "2026-02-20",
                "athlete_email": "marcus@email.com",
                "exercise": "Bench Press",
                "set_number": 1,
                "weight": 100,
                "weight_unit": "kg",
                "reps": 8,
                "rpe": 7.5,
            },
            {
                "date": "2026-02-20",
                "athlete_email": "marcus@email.com",
                "exercise": "Bench Press",
                "set_number": 2,
                "weight": 100,
                "weight_unit": "kg",
                "reps": 6,
                "rpe": 8.0,
            },
            {
                "date": "2026-02-20",
                "athlete_email": "marcus@email.com",
                "exercise": "Bench Press",
                "set_number": 3,
                "weight": 220,
                "weight_unit": "lbs",
                "reps": 5,
                "rpe": 8.5,
            },
            {
                "date": "2026-02-22",
                "athlete_email": "marcus@email.com",
                "exercise": "Deadlift",
                "set_number": 1,
                "weight": 160,
                "weight_unit": "kg",
                "reps": 4,
                "rpe": 8.5,
            },
        ]
    )


@pytest.fixture
def sample_body_metrics_df() -> pd.DataFrame:
    """Sample body metrics data."""
    return pd.DataFrame(
        [
            {
                "date": "2026-02-20",
                "weight_kg": 80.5,
                "body_fat_pct": 11.2,
                "resting_heart_rate": 55,
                "sleep_quality": 8,
                "stress_level": 3,
                "recovery_score": 85,
                "steps": 10500,
            },
            {
                "date": "2026-02-21",
                "weight_kg": 80.7,
                "body_fat_pct": 11.2,
                "resting_heart_rate": 54,
                "sleep_quality": 7,
                "stress_level": 4,
                "recovery_score": 78,
                "steps": 8200,
            },
            {
                "date": "2026-02-22",
                "weight_kg": 80.3,
                "body_fat_pct": 11.1,
                "resting_heart_rate": 56,
                "sleep_quality": 9,
                "stress_level": 2,
                "recovery_score": 90,
                "steps": 11300,
            },
            {
                "date": "2026-02-23",
                "weight_kg": 80.6,
                "body_fat_pct": 11.3,
                "resting_heart_rate": 53,
                "sleep_quality": 8,
                "stress_level": 3,
                "recovery_score": 83,
                "steps": 9600,
            },
            {
                "date": "2026-02-24",
                "weight_kg": 80.9,
                "body_fat_pct": 11.2,
                "resting_heart_rate": 55,
                "sleep_quality": 6,
                "stress_level": 5,
                "recovery_score": 72,
                "steps": 7400,
            },
            {
                "date": "2026-02-25",
                "weight_kg": 80.4,
                "body_fat_pct": 11.1,
                "resting_heart_rate": 54,
                "sleep_quality": 8,
                "stress_level": 3,
                "recovery_score": 87,
                "steps": 10800,
            },
            {
                "date": "2026-02-26",
                "weight_kg": 80.5,
                "body_fat_pct": 11.0,
                "resting_heart_rate": 53,
                "sleep_quality": 9,
                "stress_level": 2,
                "recovery_score": 91,
                "steps": 12100,
            },
        ]
    )


@pytest.fixture
def sample_nutrition_df() -> pd.DataFrame:
    """Sample nutrition data."""
    return pd.DataFrame(
        [
            {
                "log_date": "2026-02-20",
                "meal_type": "breakfast",
                "food_name": "Eggs + Toast",
                "calories": 520,
                "protein_g": 35.0,
                "carbs_g": 40.0,
                "fats_g": 22.0,
                "fiber_g": 3.0,
            },
            {
                "log_date": "2026-02-20",
                "meal_type": "lunch",
                "food_name": "Chicken Rice",
                "calories": 680,
                "protein_g": 52.0,
                "carbs_g": 70.0,
                "fats_g": 18.0,
                "fiber_g": 6.0,
            },
            {
                "log_date": "2026-02-20",
                "meal_type": "dinner",
                "food_name": "Salmon + Sweet Potato",
                "calories": 750,
                "protein_g": 48.0,
                "carbs_g": 65.0,
                "fats_g": 28.0,
                "fiber_g": 8.0,
            },
        ]
    )


@pytest.fixture
def sample_quality_rules() -> dict[str, Any]:
    """Minimal quality rules for testing the validator."""
    return {
        "schema_rules": {
            "exercises": {
                "required_columns": ["name", "primary_muscle", "exercise_type"],
                "column_types": {
                    "name": "string",
                    "primary_muscle": "string",
                },
                "severity": "CRITICAL",
            },
            "body_metrics": {
                "required_columns": ["weight_kg", "body_fat_pct"],
                "column_types": {
                    "weight_kg": "numeric",
                    "body_fat_pct": "numeric",
                },
                "severity": "CRITICAL",
            },
        },
        "business_rules": {
            "exercises": [
                {
                    "rule": "name_not_empty",
                    "column": "name",
                    "check": "not_empty",
                    "severity": "CRITICAL",
                },
                {
                    "rule": "valid_exercise_type",
                    "column": "exercise_type",
                    "check": "in_set",
                    "allowed_values": ["compound", "isolation", "cardio", "bodyweight", "olympic", "plyometric"],
                    "severity": "WARNING",
                },
            ],
            "body_metrics": [
                {
                    "rule": "weight_reasonable",
                    "column": "weight_kg",
                    "check": "range",
                    "min": 30.0,
                    "max": 300.0,
                    "severity": "WARNING",
                },
                {
                    "rule": "body_fat_reasonable",
                    "column": "body_fat_pct",
                    "check": "range",
                    "min": 2.0,
                    "max": 60.0,
                    "severity": "WARNING",
                },
            ],
        },
        "freshness_rules": {
            "body_metrics": {
                "max_age_hours": 48,
                "severity": "WARNING",
            },
        },
        "anomaly_detection": {
            "z_score_threshold": 3.0,
            "iqr_multiplier": 1.5,
        },
    }


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory with the full layer structure."""
    (tmp_path / "bronze").mkdir()
    (tmp_path / "silver").mkdir()
    (tmp_path / "gold").mkdir()
    (tmp_path / "incoming").mkdir()
    (tmp_path / "incoming" / "processed").mkdir()
    (tmp_path / "incoming" / "errors").mkdir()
    return tmp_path
