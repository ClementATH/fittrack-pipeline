"""
Data Contract Tests
====================

Tests for Pydantic contract models, the ContractEnforcer, and JSON Schema
generation. Validates that contracts catch invalid data and pass valid data.
"""

import json
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from src.quality.contracts.enforcer import ContractEnforcer
from src.quality.contracts.models import (
    BodyMetricContract,
    ExerciseContract,
    NutritionLogContract,
    WorkoutContract,
)


# ============================================================
# Contract model unit tests
# ============================================================
class TestExerciseContract:
    def test_valid_exercise(self) -> None:
        ex = ExerciseContract(
            name="Bench Press",
            primary_muscle="chest",
            exercise_type="compound",
            equipment="barbell",
            difficulty="intermediate",
        )
        assert ex.name == "Bench Press"
        assert ex.primary_muscle == "chest"

    def test_rejects_empty_name(self) -> None:
        with pytest.raises(ValidationError, match="String should have at least 1 character"):
            ExerciseContract(
                name="",
                primary_muscle="chest",
                exercise_type="compound",
                equipment="barbell",
            )

    def test_rejects_invalid_muscle_group(self) -> None:
        with pytest.raises(ValidationError, match="Invalid muscle group"):
            ExerciseContract(
                name="Mystery Lift",
                primary_muscle="toenails",
                exercise_type="compound",
                equipment="barbell",
            )


class TestWorkoutContract:
    def test_valid_workout(self) -> None:
        w = WorkoutContract(
            athlete_id="athlete_123",
            workout_date="2026-02-20",
            status="completed",
        )
        assert w.status == "completed"

    def test_rejects_invalid_status(self) -> None:
        with pytest.raises(ValidationError, match="Invalid status"):
            WorkoutContract(
                athlete_id="athlete_123",
                workout_date="2026-02-20",
                status="abandoned",
            )


class TestBodyMetricContract:
    def test_valid_body_metric(self) -> None:
        bm = BodyMetricContract(weight_kg=80.5, body_fat_pct=11.2)
        assert bm.weight_kg == 80.5

    def test_rejects_weight_out_of_range(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal to 30"):
            BodyMetricContract(weight_kg=5.0)

    def test_rejects_high_weight(self) -> None:
        with pytest.raises(ValidationError, match="less than or equal to 300"):
            BodyMetricContract(weight_kg=500.0)


class TestNutritionLogContract:
    def test_valid_nutrition_log(self) -> None:
        nl = NutritionLogContract(
            log_date="2026-02-20",
            meal_type="breakfast",
            calories=520,
            protein_g=35.0,
        )
        assert nl.calories == 520

    def test_rejects_negative_calories(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            NutritionLogContract(
                log_date="2026-02-20",
                meal_type="breakfast",
                calories=-100,
            )

    def test_rejects_invalid_meal_type(self) -> None:
        with pytest.raises(ValidationError, match="Invalid meal type"):
            NutritionLogContract(
                log_date="2026-02-20",
                meal_type="midnight_feast",
                calories=500,
            )


# ============================================================
# ContractEnforcer tests
# ============================================================
class TestContractEnforcer:
    def test_enforce_valid_dataframe_passes(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "name": "Bench Press",
                    "primary_muscle": "chest",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
                {
                    "name": "Squat",
                    "primary_muscle": "quads",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
            ]
        )
        enforcer = ContractEnforcer()
        result = enforcer.enforce(df, "exercises")
        assert result.passed is True
        assert result.valid_rows == 2
        assert result.violation_rate == 0.0

    def test_enforce_invalid_dataframe_returns_violations(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "name": "Good Exercise",
                    "primary_muscle": "chest",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
                {
                    "name": "",
                    "primary_muscle": "toenails",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
            ]
        )
        enforcer = ContractEnforcer()
        result = enforcer.enforce(df, "exercises")
        assert result.passed is False
        assert result.valid_rows == 1
        assert len(result.violations) > 0

    def test_enforce_unknown_table_raises(self) -> None:
        enforcer = ContractEnforcer()
        with pytest.raises(KeyError, match="No contract registered"):
            enforcer.enforce(pd.DataFrame(), "unknown_table")

    def test_enforce_empty_dataframe(self) -> None:
        enforcer = ContractEnforcer()
        result = enforcer.enforce(pd.DataFrame(), "exercises")
        assert result.passed is True
        assert result.total_rows == 0
        assert result.violation_rate == 0.0

    def test_violation_rate_calculation(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "name": "Good",
                    "primary_muscle": "chest",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
                {
                    "name": "",
                    "primary_muscle": "invalid",
                    "exercise_type": "compound",
                    "equipment": "barbell",
                    "difficulty": "intermediate",
                },
            ]
        )
        enforcer = ContractEnforcer()
        result = enforcer.enforce(df, "exercises")
        assert result.violation_rate == 50.0

    def test_result_to_dict(self) -> None:
        enforcer = ContractEnforcer()
        result = enforcer.enforce(
            pd.DataFrame(
                [
                    {
                        "name": "Press",
                        "primary_muscle": "shoulders",
                        "exercise_type": "compound",
                        "equipment": "barbell",
                        "difficulty": "easy",
                    },
                ]
            ),
            "exercises",
        )
        d = result.to_dict()
        assert "table_name" in d
        assert "violation_count" in d
        assert "passed" in d


# ============================================================
# JSON Schema generation tests
# ============================================================
class TestJsonSchemaGeneration:
    def test_generate_schemas_creates_files(self, tmp_path: Path) -> None:
        generated = ContractEnforcer.generate_json_schemas(output_dir=str(tmp_path))
        assert len(generated) == 4
        for table_name, file_path in generated.items():
            assert Path(file_path).exists()
            assert table_name in file_path

    def test_generated_schema_is_valid_json(self, tmp_path: Path) -> None:
        ContractEnforcer.generate_json_schemas(output_dir=str(tmp_path))
        for schema_file in tmp_path.glob("*.schema.json"):
            content = json.loads(schema_file.read_text())
            assert "properties" in content
            assert "title" in content
