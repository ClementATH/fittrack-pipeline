"""
Tests: Silver Layer (Transformation)
======================================

Tests for the transformation module: DataCleaner, DataTransformer, DataEnricher.

# LEARN: Transformation tests verify business logic — the rules that
# convert raw data into analytics-ready data. These are the most
# important tests because wrong transformations = wrong dashboards.
"""

import pandas as pd
import pytest

from src.transformation.cleaner import DataCleaner
from src.transformation.transformer import DataTransformer, WGER_MUSCLE_MAP, WGER_EQUIPMENT_MAP
from src.transformation.enricher import DataEnricher


# ============================================================
# DataCleaner Tests
# ============================================================


class TestSnakeCase:
    """Test the to_snake_case conversion — the foundation of column standardization."""

    @pytest.mark.parametrize(
        "input_name, expected",
        [
            ("userName", "user_name"),
            ("UserName", "user_name"),
            ("user_name", "user_name"),
            ("User Name", "user_name"),
            ("user-name", "user_name"),
            ("UserID", "user_id"),
            ("HTTPResponse", "http_response"),
            ("getHTTPResponse", "get_http_response"),
            ("already_snake_case", "already_snake_case"),
            ("", ""),
            ("ALLCAPS", "allcaps"),
            ("simple", "simple"),
            ("with.dots", "with_dots"),
        ],
    )
    def test_to_snake_case(self, input_name: str, expected: str):
        """to_snake_case should correctly convert various naming conventions."""
        assert DataCleaner.to_snake_case(input_name) == expected


class TestDataCleaner:
    """Test the full DataCleaner pipeline."""

    def test_standardize_columns(self):
        """Column names should be converted to snake_case."""
        df = pd.DataFrame({"UserName": ["a"], "exerciseType": ["b"], "BodyFatPct": [10]})
        cleaner = DataCleaner()
        result = cleaner.standardize_columns(df)
        assert list(result.columns) == ["user_name", "exercise_type", "body_fat_pct"]

    def test_standardize_handles_duplicate_names(self):
        """Duplicate column names after conversion should be disambiguated."""
        df = pd.DataFrame({"UserName": ["a"], "user_name": ["b"]})
        cleaner = DataCleaner()
        result = cleaner.standardize_columns(df)
        # Both should exist without collision
        assert len(result.columns) == 2
        assert result.columns[0] != result.columns[1]

    def test_trim_strings(self):
        """Whitespace should be trimmed from all string columns."""
        df = pd.DataFrame({"name": ["  Squat  ", " Bench"], "reps": [5, 8]})
        result = DataCleaner.trim_strings(df)
        assert result["name"].iloc[0] == "Squat"
        assert result["name"].iloc[1] == "Bench"

    def test_handle_nulls_numeric_fill(self):
        """Numeric nulls should be filled when numeric_fill is specified."""
        df = pd.DataFrame({"weight": [100.0, None, 80.0], "name": ["a", None, "c"]})
        result = DataCleaner.handle_nulls(df, numeric_fill=0.0)
        assert result["weight"].iloc[1] == 0.0
        # String nulls should remain
        assert result["name"].iloc[1] is None

    def test_handle_nulls_no_fill_preserves_nulls(self):
        """Without fill values, nulls should remain."""
        df = pd.DataFrame({"weight": [100.0, None, 80.0]})
        result = DataCleaner.handle_nulls(df)
        assert pd.isna(result["weight"].iloc[1])

    def test_cast_types_string(self):
        """String casting should work correctly."""
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = DataCleaner.cast_types(df, {"id": "string"})
        assert result["id"].dtype == object

    def test_cast_types_integer(self):
        """Integer casting should handle nullable integers."""
        df = pd.DataFrame({"count": ["1", "2", "3"]})
        result = DataCleaner.cast_types(df, {"count": "integer"})
        assert result["count"].iloc[0] == 1

    def test_cast_types_float(self):
        """Float casting should convert strings to numbers."""
        df = pd.DataFrame({"weight": ["80.5", "75.2"]})
        result = DataCleaner.cast_types(df, {"weight": "float"})
        assert result["weight"].iloc[0] == pytest.approx(80.5)

    def test_cast_types_missing_column_ignored(self):
        """Casting for a column that doesn't exist should be silently ignored."""
        df = pd.DataFrame({"a": [1]})
        result = DataCleaner.cast_types(df, {"nonexistent": "string"})
        assert len(result.columns) == 1  # No error, no change

    def test_deduplicate_removes_exact_dupes(self):
        """Exact duplicate rows should be removed."""
        df = pd.DataFrame({"name": ["Squat", "Squat", "Bench"], "reps": [5, 5, 8]})
        result = DataCleaner.deduplicate(df)
        assert len(result) == 2

    def test_deduplicate_keeps_last_by_default(self):
        """By default, the last duplicate should be kept."""
        df = pd.DataFrame({
            "name": ["Squat", "Squat"],
            "version": [1, 2],
        })
        result = DataCleaner.deduplicate(df, subset=["name"], keep="last")
        assert result["version"].iloc[0] == 2

    def test_deduplicate_ignores_metadata_columns(self):
        """Columns starting with '_' should be excluded from dedup comparison."""
        df = pd.DataFrame({
            "name": ["Squat", "Squat"],
            "_batch_id": ["batch1", "batch2"],
        })
        result = DataCleaner.deduplicate(df)
        # Both rows have same "name" so one should be removed
        assert len(result) == 1

    def test_full_clean_pipeline(self, sample_exercises_df: pd.DataFrame):
        """The full clean() method should apply all steps without error."""
        cleaner = DataCleaner()
        result = cleaner.clean(sample_exercises_df, table_name="exercises")
        # All column names should be snake_case
        for col in result.columns:
            assert col == col.lower()
            assert " " not in col


# ============================================================
# DataTransformer Tests
# ============================================================


class TestDataTransformer:
    """Test source-specific transformations."""

    def test_wger_muscle_map_completeness(self):
        """The muscle map should contain the most common Wger muscle IDs."""
        assert 1 in WGER_MUSCLE_MAP  # Biceps
        assert 4 in WGER_MUSCLE_MAP  # Quads
        assert 5 in WGER_MUSCLE_MAP  # Chest
        assert 10 in WGER_MUSCLE_MAP  # Glutes

    def test_wger_equipment_map_completeness(self):
        """The equipment map should cover common equipment types."""
        assert 1 in WGER_EQUIPMENT_MAP  # Barbell
        assert 3 in WGER_EQUIPMENT_MAP  # Dumbbell
        assert 5 in WGER_EQUIPMENT_MAP  # Bodyweight

    def test_transform_wger_exercises_slug_generation(self, sample_exercises_df: pd.DataFrame):
        """Wger transformer should generate proper slugs from names."""
        transformer = DataTransformer()
        result = transformer.transform_wger_exercises(sample_exercises_df)
        assert "slug" in result.columns
        # Slugs should be lowercase with hyphens
        for slug in result["slug"]:
            assert slug == slug.lower()
            assert " " not in slug

    def test_transform_wger_exercises_muscle_mapping(self, sample_exercises_df: pd.DataFrame):
        """Wger transformer should map muscle IDs to human-readable names."""
        transformer = DataTransformer()
        result = transformer.transform_wger_exercises(sample_exercises_df)
        assert "primary_muscle" in result.columns
        # All mapped muscles should be valid strings
        for muscle in result["primary_muscle"]:
            assert isinstance(muscle, str)
            assert len(muscle) > 0

    def test_transform_wger_exercises_equipment_mapping(self, sample_exercises_df: pd.DataFrame):
        """Wger transformer should map equipment IDs to names."""
        transformer = DataTransformer()
        result = transformer.transform_wger_exercises(sample_exercises_df)
        assert "equipment" in result.columns

    def test_transform_file_workouts_lbs_to_kg(self, sample_workouts_df: pd.DataFrame):
        """File workout transformer should convert lbs to kg."""
        transformer = DataTransformer()
        result = transformer.transform_file_workouts(sample_workouts_df)
        # Row 2 has 220 lbs, should be ~99.79 kg
        lbs_rows = sample_workouts_df[sample_workouts_df["weight_unit"].str.lower() == "lbs"]
        if len(lbs_rows) > 0:
            idx = lbs_rows.index[0]
            original_lbs = sample_workouts_df.loc[idx, "weight"]
            converted_kg = result.loc[idx, "weight"]
            assert converted_kg == pytest.approx(original_lbs * 0.453592, rel=0.01)

    def test_transform_passthrough_for_unknown_source(self):
        """Unknown source/dataset combos should pass through unchanged."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        transformer = DataTransformer()
        result = transformer.transform(df, source="unknown", dataset="unknown")
        pd.testing.assert_frame_equal(result, df)

    def test_store_silver_creates_parquet(self, tmp_path: Path):
        """Silver storage should create a valid Parquet file."""
        df = pd.DataFrame({"name": ["Squat"], "reps": [5]})
        transformer = DataTransformer(silver_path=str(tmp_path / "silver"))
        path = transformer.store_silver(df, "test_source", "exercises")
        assert path.exists()
        assert path.suffix == ".parquet"
        # Should be readable
        df_read = pd.read_parquet(path)
        assert len(df_read) == 1


# ============================================================
# DataEnricher Tests
# ============================================================


class TestDataEnricher:
    """Test enrichment with derived computed fields."""

    def test_enrich_exercises_compound_score(self, sample_exercises_df: pd.DataFrame):
        """Exercises with more secondary muscles should get higher compound scores."""
        # First transform to get secondary_muscles column
        transformer = DataTransformer()
        transformed = transformer.transform_wger_exercises(sample_exercises_df)

        enricher = DataEnricher()
        result = enricher.enrich_exercises(transformed)
        assert "compound_score" in result.columns
        assert all(result["compound_score"] >= 1)

    def test_enrich_exercises_movement_category(self, sample_exercises_df: pd.DataFrame):
        """Exercises should be categorized into movement groups."""
        transformer = DataTransformer()
        transformed = transformer.transform_wger_exercises(sample_exercises_df)

        enricher = DataEnricher()
        result = enricher.enrich_exercises(transformed)
        assert "movement_category" in result.columns
        valid_categories = {
            "upper_body_push", "upper_body_pull", "lower_body",
            "core", "full_body", "other",
        }
        for cat in result["movement_category"]:
            assert cat in valid_categories

    def test_enrich_body_metrics_fat_mass(self, sample_body_metrics_df: pd.DataFrame):
        """Fat mass should be calculated as weight * body_fat_pct / 100."""
        enricher = DataEnricher()
        result = enricher.enrich_body_metrics(sample_body_metrics_df)
        assert "fat_mass_kg" in result.columns
        assert "lean_mass_calc_kg" in result.columns
        # Verify first row: 80.5 * 11.2 / 100 = ~9.0
        assert result["fat_mass_kg"].iloc[0] == pytest.approx(80.5 * 11.2 / 100, abs=0.2)

    def test_enrich_body_metrics_weight_change(self, sample_body_metrics_df: pd.DataFrame):
        """Weight change should be the day-over-day difference."""
        enricher = DataEnricher()
        result = enricher.enrich_body_metrics(sample_body_metrics_df)
        assert "weight_change_kg" in result.columns
        # First row should be NaN (no previous day)
        assert pd.isna(result["weight_change_kg"].iloc[0])

    def test_enrich_body_metrics_recovery_index(self, sample_body_metrics_df: pd.DataFrame):
        """Recovery index should be computed from sleep, stress, and recovery."""
        enricher = DataEnricher()
        result = enricher.enrich_body_metrics(sample_body_metrics_df)
        assert "recovery_index" in result.columns
        # Should be between 0 and 10 (normalized)
        for val in result["recovery_index"].dropna():
            assert 0 <= val <= 100

    def test_enrich_nutrition_macro_percentages(self, sample_nutrition_df: pd.DataFrame):
        """Nutrition enrichment should compute macro split percentages."""
        enricher = DataEnricher()
        result = enricher.enrich_nutrition(sample_nutrition_df)
        assert "protein_pct" in result.columns
        assert "carbs_pct" in result.columns
        assert "fats_pct" in result.columns
        # Percentages should roughly sum to 100
        for _, row in result.iterrows():
            total = (row.get("protein_pct", 0) or 0) + (row.get("carbs_pct", 0) or 0) + (row.get("fats_pct", 0) or 0)
            assert 95 <= total <= 105  # Allow small rounding error

    def test_enrich_nutrition_calorie_contributions(self, sample_nutrition_df: pd.DataFrame):
        """Calorie contributions should follow 4/4/9 rule."""
        enricher = DataEnricher()
        result = enricher.enrich_nutrition(sample_nutrition_df)
        # Row 0: protein=35g * 4 = 140 cal
        assert result["calories_from_protein"].iloc[0] == pytest.approx(35.0 * 4, abs=1)
        # Row 0: fats=22g * 9 = 198 cal
        assert result["calories_from_fats"].iloc[0] == pytest.approx(22.0 * 9, abs=1)

    def test_enrich_adds_common_fields(self, sample_exercises_df: pd.DataFrame):
        """All enriched DataFrames should have 'id' and '_processed_at' columns."""
        enricher = DataEnricher()
        result = enricher.enrich(sample_exercises_df, dataset="exercises")
        assert "id" in result.columns
        assert "_processed_at" in result.columns

    def test_enrich_unknown_dataset_passthrough(self):
        """Unknown datasets should pass through with only common fields added."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        enricher = DataEnricher()
        result = enricher.enrich(df, dataset="unknown_type")
        assert "a" in result.columns
        assert "id" in result.columns
