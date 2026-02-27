"""
Data Transformer (Silver Layer — Step 2)
==========================================

WHAT: Applies source-specific transformations to cleaned data. Each data
source (Wger, USDA, file uploads) needs different transformations to map
its raw structure to our target Gold schema.

WHY: Cleaning (Step 1) handles generic issues. Transformation handles
source-specific logic:
  - Wger API returns muscle IDs — we need to map them to muscle names
  - USDA API returns nested nutrient arrays — we need to flatten them
  - File uploads might use pounds — we need to convert to kilograms

# LEARN: The distinction between cleaning and transformation is important:
#   Cleaning = generic operations (rename cols, cast types, dedup)
#   Transformation = business logic (unit conversion, field mapping, enrichment)
# In the Medallion Architecture, both happen in the Silver layer, but
# separating them makes your code testable and maintainable.

What Would Break If you mixed cleaning with transformation:
  - Testing becomes hard (can't test unit conversion without also testing dedup)
  - Reusability drops (cleaning logic is universal, transformation is source-specific)
  - Debugging is harder (is the bug in the cleaning or the business logic?)
"""

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logger import get_logger

logger = get_logger("fittrack.transformation.transformer")


# ============================================================
# Mapping tables: API values -> Gold schema values
# ============================================================
# LEARN: Mapping tables are lookup dictionaries that translate
# external system values into your internal standardized values.
# This is essentially what a "dimension table" does in a warehouse.
# ============================================================

WGER_MUSCLE_MAP: dict[int, str] = {
    1: "biceps",
    2: "shoulders",
    3: "abs",
    4: "quads",
    5: "chest",
    6: "triceps",
    7: "lats",
    8: "back",
    9: "hamstrings",
    10: "glutes",
    11: "calves",
    12: "traps",
    13: "forearms",
    14: "obliques",
    15: "hip_flexors",
    16: "adductors",
    17: "abductors",
    18: "neck",
}

WGER_EQUIPMENT_MAP: dict[int, str] = {
    1: "barbell",
    2: "smith_machine",
    3: "dumbbell",
    4: "machine",
    5: "bodyweight",
    6: "pull_up_bar",
    7: "none",
    8: "cable",
    9: "ez_bar",
    10: "kettlebell",
}


class DataTransformer:
    """
    Applies source-specific transformations to cleaned data.

    Each transform_* method handles a specific source/dataset combination.
    The main transform() method routes to the appropriate transformer.

    Usage:
        transformer = DataTransformer(silver_path="data/silver")
        result_df = transformer.transform(df, source="wger_exercises", dataset="exercises")
    """

    def __init__(self, silver_path: str = "data/silver"):
        self.silver_path = Path(silver_path)
        self.silver_path.mkdir(parents=True, exist_ok=True)

    def transform(
        self,
        df: pd.DataFrame,
        source: str,
        dataset: str,
    ) -> pd.DataFrame:
        """
        Route to the appropriate transformer based on source and dataset.

        Args:
            df: Cleaned DataFrame from the cleaner
            source: Source identifier (e.g., 'wger_exercises')
            dataset: Dataset within the source (e.g., 'exercises')

        Returns:
            Transformed DataFrame ready for the Gold layer
        """
        transform_key = f"{source}__{dataset}"
        transform_methods: dict[str, Any] = {
            "wger_exercises__exercises": self.transform_wger_exercises,
            "wger_exercises__muscles": self.transform_wger_muscles,
            "wger_exercises__equipment": self.transform_wger_equipment,
            "usda_nutrition__foods_search": self.transform_usda_foods,
            "file_drop_zone__workout_logs": self.transform_file_workouts,
            "file_drop_zone__body_metrics": self.transform_file_body_metrics,
            "file_drop_zone__nutrition_logs": self.transform_file_nutrition,
        }

        method = transform_methods.get(transform_key)
        if method is None:
            logger.warning(
                f"No specific transformer for {transform_key}, "
                f"passing through with minimal transforms",
                extra={"source": source, "layer": "silver"},
            )
            return df

        logger.info(
            f"Transforming {source}/{dataset}: {len(df)} rows",
            extra={"source": source, "layer": "silver"},
        )

        result = method(df)

        logger.info(
            f"Transformation complete for {source}/{dataset}: "
            f"{len(result)} rows, {len(result.columns)} columns",
            extra={"source": source, "layer": "silver"},
        )
        return result

    def store_silver(
        self,
        df: pd.DataFrame,
        source: str,
        dataset: str,
    ) -> Path:
        """
        Store transformed data as Parquet in the Silver layer.

        # LEARN: Silver layer files use a simpler naming convention
        # than Bronze (no timestamps) because Silver represents the
        # "current clean version" of the data, not a historical record.
        """
        source_dir = self.silver_path / source
        source_dir.mkdir(parents=True, exist_ok=True)

        file_path = source_dir / f"{dataset}.parquet"

        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")

        logger.info(
            f"Stored {len(df)} rows to Silver: {file_path}",
            extra={"source": source, "layer": "silver"},
        )
        return file_path

    # ============================================================
    # Wger API Transformers
    # ============================================================

    def transform_wger_exercises(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Wger exercise data to match our Gold schema.

        Wger API response structure:
          - id, name, description, muscles (list of IDs), equipment (list of IDs)
          - category (int ID mapped to exercise type)
          - language (filter for English)

        Target: gold_exercises table
        """
        df = df.copy()

        # Map muscle IDs to names
        if "muscles" in df.columns:
            df["primary_muscle"] = df["muscles"].apply(
                lambda x: WGER_MUSCLE_MAP.get(x[0], "full_body")
                if isinstance(x, list) and len(x) > 0
                else "full_body"
            )
            df["secondary_muscles"] = df["muscles"].apply(
                lambda x: [WGER_MUSCLE_MAP.get(m, "unknown") for m in x[1:]]
                if isinstance(x, list) and len(x) > 1
                else []
            )

        # Map equipment IDs to names
        if "equipment" in df.columns:
            df["equipment_name"] = df["equipment"].apply(
                lambda x: WGER_EQUIPMENT_MAP.get(x[0], "none")
                if isinstance(x, list) and len(x) > 0
                else "none"
            )

        # Generate slug from name
        if "name" in df.columns:
            df["slug"] = (
                df["name"]
                .str.lower()
                .str.replace(r"[^a-z0-9\s]", "", regex=True)
                .str.replace(r"\s+", "-", regex=True)
                .str.strip("-")
            )

        # Map Wger category to our exercise_type enum
        category_map: dict[int, str] = {
            8: "compound",  # Arms
            9: "compound",  # Legs
            10: "compound", # Abs
            11: "compound", # Chest
            12: "compound", # Back
            13: "compound", # Shoulders
            14: "cardio",   # Calves -> treat as isolation but keep generic
            15: "compound", # Glutes
        }
        if "category" in df.columns:
            df["exercise_type"] = df["category"].apply(
                lambda x: category_map.get(
                    x if isinstance(x, int) else 0, "compound"
                )
            )

        # Set defaults
        df["difficulty"] = "intermediate"
        df["is_unilateral"] = False
        df["is_custom"] = False

        # Select and rename columns to match Gold schema
        column_map = {
            "id": "source_id",
            "name": "name",
            "slug": "slug",
            "primary_muscle": "primary_muscle",
            "secondary_muscles": "secondary_muscles",
            "exercise_type": "exercise_type",
            "equipment_name": "equipment",
            "difficulty": "difficulty",
            "description": "instructions",
            "is_unilateral": "is_unilateral",
            "is_custom": "is_custom",
        }

        available_cols = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=available_cols)

        # Keep only mapped columns plus metadata
        keep_cols = list(available_cols.values()) + [
            col for col in df.columns if col.startswith("_")
        ]
        df = df[[col for col in keep_cols if col in df.columns]]

        return df

    def transform_wger_muscles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Wger muscle data (reference/lookup table)."""
        df = df.copy()
        if "name_en" in df.columns:
            df = df.rename(columns={"name_en": "name"})
        elif "name" not in df.columns and "name_en" not in df.columns:
            # Try to use whatever name column exists
            name_cols = [c for c in df.columns if "name" in c.lower()]
            if name_cols:
                df = df.rename(columns={name_cols[0]: "name"})
        return df

    def transform_wger_equipment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform Wger equipment data (reference/lookup table)."""
        return self.transform_wger_muscles(df)  # Same structure

    # ============================================================
    # USDA API Transformers
    # ============================================================

    def transform_usda_foods(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform USDA food search results to match our nutrition schema.

        USDA API returns nested structures with nutrient arrays.
        We flatten these into simple columns matching nutrition_logs.

        # LEARN: API normalization (flattening nested JSON) is one of
        # the most common transformation tasks. The USDA API returns
        # nutrients as an array of objects; we need individual columns.
        """
        df = df.copy()

        # Extract key nutrients from nested structure if present
        nutrient_map = {
            1003: "protein_g",   # Protein
            1004: "fats_g",      # Total fat
            1005: "carbs_g",     # Carbohydrates
            1008: "calories",    # Energy (kcal)
            1079: "fiber_g",     # Fiber
            2000: "sugar_g",     # Sugars
            1093: "sodium_mg",   # Sodium
        }

        if "food_nutrients" in df.columns:
            for _, row in df.iterrows():
                nutrients = row.get("food_nutrients", [])
                if isinstance(nutrients, list):
                    for nutrient in nutrients:
                        if isinstance(nutrient, dict):
                            nid = nutrient.get("nutrientId") or nutrient.get(
                                "nutrient", {}
                            ).get("id")
                            if nid in nutrient_map:
                                col_name = nutrient_map[nid]
                                if col_name not in df.columns:
                                    df[col_name] = None
                                df.at[row.name, col_name] = nutrient.get(
                                    "value", nutrient.get("amount")
                                )

        # Rename description to food_name
        if "description" in df.columns:
            df = df.rename(columns={"description": "food_name"})
        if "fdc_id" in df.columns:
            df = df.rename(columns={"fdc_id": "usda_fdc_id"})

        return df

    # ============================================================
    # File-Based Transformers
    # ============================================================

    def transform_file_workouts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform workout log files to match Gold workout schema.

        Expected file columns: date, exercise, sets, reps, weight, unit, etc.
        """
        df = df.copy()

        # Convert pounds to kilograms if needed
        if "weight_unit" in df.columns or "unit" in df.columns:
            unit_col = "weight_unit" if "weight_unit" in df.columns else "unit"
            mask = df[unit_col].str.lower().isin(["lbs", "lb", "pounds"])
            weight_col = "weight" if "weight" in df.columns else "weight_kg"
            if weight_col in df.columns:
                df.loc[mask, weight_col] = df.loc[mask, weight_col] * 0.453592
                # Standardize unit
                df[unit_col] = "kg"

        # Convert date strings to proper dates
        date_cols = [c for c in df.columns if "date" in c.lower()]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        return df

    def transform_file_body_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform body metric files to match Gold body_metrics schema.

        Handles unit conversions (lbs->kg, inches->cm).
        """
        df = df.copy()

        # Weight: convert lbs to kg
        if "weight_unit" in df.columns:
            mask = df["weight_unit"].str.lower().isin(["lbs", "lb", "pounds"])
            if "weight" in df.columns:
                df.loc[mask, "weight"] = df.loc[mask, "weight"] * 0.453592
            if "weight_kg" in df.columns:
                df.loc[mask, "weight_kg"] = df.loc[mask, "weight_kg"] * 0.453592

        # Measurements: convert inches to cm
        cm_cols = [c for c in df.columns if "_in" in c or "_inches" in c]
        for col in cm_cols:
            new_col = col.replace("_in", "_cm").replace("_inches", "_cm")
            df[new_col] = pd.to_numeric(df[col], errors="coerce") * 2.54
            df = df.drop(columns=[col])

        # Convert date strings
        date_cols = [c for c in df.columns if "date" in c.lower() or "measured" in c.lower()]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        return df

    def transform_file_nutrition(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform nutrition file data to match Gold nutrition_logs schema."""
        df = df.copy()

        # Ensure numeric columns
        numeric_cols = ["calories", "protein_g", "carbs_g", "fats_g", "fiber_g"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert date strings
        date_cols = [c for c in df.columns if "date" in c.lower()]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        return df
