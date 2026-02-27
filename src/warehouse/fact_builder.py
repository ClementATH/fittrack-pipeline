"""
Fact Table Builder (Gold Layer)
================================

WHAT: Builds fact tables — the measurable events at the center of the
star schema. Facts capture the "how much" and "how many" of business events.

WHY: Fact tables store the metrics that drive analytics:
  - fact_workout_sets: Every set logged (weight, reps, RPE)
  - fact_body_metrics: Daily body measurements
  - fact_nutrition: Every meal logged
  - fact_workouts: Aggregated workout sessions

# LEARN: In dimensional modeling, facts are the numeric, additive
# measurements at the intersection of dimensions:
#
#   fact_workout_sets has:
#     - dimension keys: exercise_id, athlete_id, date_key
#     - measures: weight_kg, reps, volume (weight * reps)
#
# Facts are typically the largest tables in your warehouse.
# At WellMed, fact tables might be: fact_patient_visits,
# fact_lab_results, fact_claims. They capture EVENTS.
#
# Three types of facts:
#   1. Transactional: One row per event (each set, each meal)
#   2. Periodic Snapshot: One row per time period (daily metrics)
#   3. Accumulating Snapshot: One row per process (full workout lifecycle)
"""

import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logger import get_logger

logger = get_logger("fittrack.warehouse.fact")


class FactBuilder:
    """
    Builds and maintains fact tables in the Gold layer.

    Usage:
        builder = FactBuilder(gold_path="data/gold")
        builder.build_fact_workouts(silver_df)
        builder.build_fact_body_metrics(silver_df)
    """

    def __init__(self, gold_path: str = "data/gold"):
        self.gold_path = Path(gold_path)
        self.gold_path.mkdir(parents=True, exist_ok=True)

    def _store_fact(self, df: pd.DataFrame, fact_name: str) -> Path:
        """Store a fact table as Parquet in the Gold layer."""
        file_path = self.gold_path / f"{fact_name}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")
        logger.info(
            f"Stored fact {fact_name}: {len(df)} rows -> {file_path}",
            extra={"layer": "gold"},
        )
        return file_path

    def build_fact_workouts(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the workout fact table (periodic snapshot).

        One row per workout session with aggregated metrics.
        This is a "periodic snapshot" fact because it represents
        the state of a workout at completion time.

        # LEARN: This fact table captures the WORKOUT SESSION level.
        # Each row answers: "What happened in this training session?"
        # It's connected to dimensions via foreign keys (athlete_id, date).
        """
        df = silver_df.copy()

        # Ensure IDs exist
        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Create date key for joining to dim_date
        if "workout_date" in df.columns:
            df["date_key"] = pd.to_datetime(
                df["workout_date"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        # Ensure numeric measures
        numeric_cols = [
            "duration_minutes", "total_volume_kg", "total_sets",
            "energy_level", "pump_rating", "sleep_hours_prior",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Select fact columns
        fact_cols = [
            "id", "athlete_id", "program_id", "date_key", "workout_date",
            "day_name", "status", "duration_minutes", "total_volume_kg",
            "total_sets", "energy_level", "pump_rating", "sleep_hours_prior",
            "notes",
        ]
        # Add enrichment columns if present
        for col in ["day_of_week", "week_number", "volume_per_set", "duration_category"]:
            if col in df.columns:
                fact_cols.append(col)

        df = df[[c for c in fact_cols if c in df.columns]]

        self._store_fact(df, "fact_workouts")
        logger.info(f"Built fact_workouts: {len(df)} workout sessions")
        return df

    def build_fact_body_metrics(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the body metrics fact table (periodic snapshot).

        One row per measurement day. This tracks body composition over time.

        # LEARN: Body metrics is a classic "periodic snapshot" fact.
        # Unlike transactional facts (one row per event), snapshots
        # capture the state of something at regular intervals.
        # At WellMed, patient vitals taken at each visit follow
        # the same pattern.
        """
        df = silver_df.copy()

        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Create date key
        date_col = "measured_at" if "measured_at" in df.columns else "date"
        if date_col in df.columns:
            df["date_key"] = pd.to_datetime(
                df[date_col], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        # Ensure numeric measures
        numeric_cols = [
            "weight_kg", "body_fat_pct", "lean_mass_kg", "waist_cm",
            "chest_cm", "resting_heart_rate", "sleep_quality",
            "stress_level", "recovery_score", "steps",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        fact_cols = [
            "id", "athlete_id", "date_key", "measured_at",
            "weight_kg", "body_fat_pct", "lean_mass_kg",
            "waist_cm", "chest_cm", "resting_heart_rate",
            "sleep_quality", "stress_level", "recovery_score", "steps",
        ]
        # Add enrichment columns
        for col in ["fat_mass_kg", "lean_mass_calc_kg", "weight_change_kg", "recovery_index"]:
            if col in df.columns:
                fact_cols.append(col)

        df = df[[c for c in fact_cols if c in df.columns]]

        self._store_fact(df, "fact_body_metrics")
        logger.info(f"Built fact_body_metrics: {len(df)} measurements")
        return df

    def build_fact_nutrition(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the nutrition fact table (transactional).

        One row per meal logged. This is a transactional fact — each
        row represents a single business event (logging a meal).

        # LEARN: Transactional facts are the most common type. Each row
        # is a discrete event that happened at a point in time.
        # Examples at WellMed: each claim submission, each lab result,
        # each prescription filled.
        """
        df = silver_df.copy()

        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        date_col = "log_date" if "log_date" in df.columns else "date"
        if date_col in df.columns:
            df["date_key"] = pd.to_datetime(
                df[date_col], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        # Ensure numeric measures
        for col in ["calories", "protein_g", "carbs_g", "fats_g", "fiber_g", "water_ml"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        fact_cols = [
            "id", "athlete_id", "date_key", "log_date",
            "meal_type", "meal_name", "calories",
            "protein_g", "carbs_g", "fats_g", "fiber_g",
            "sodium_mg", "sugar_g", "water_ml",
        ]
        # Add enrichment columns
        for col in [
            "protein_pct", "carbs_pct", "fats_pct", "total_macros_g",
            "calories_from_protein", "calories_from_carbs", "calories_from_fats",
        ]:
            if col in df.columns:
                fact_cols.append(col)

        df = df[[c for c in fact_cols if c in df.columns]]

        self._store_fact(df, "fact_nutrition")
        logger.info(f"Built fact_nutrition: {len(df)} meal logs")
        return df
