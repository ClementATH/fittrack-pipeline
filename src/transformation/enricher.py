"""
Data Enricher (Silver Layer — Step 3)
======================================

WHAT: Adds derived columns, computed fields, and cross-references that
make the data more useful for analytics but aren't part of the raw source.

WHY: Raw data often needs context to be useful. Adding derived fields in
the Silver layer means:
  1. Every downstream consumer gets the same calculations
  2. Computed fields are calculated once, not in every query
  3. Business logic is centralized (DRY principle)

# LEARN: Enrichment is where you add intelligence to raw data. Examples:
#   - Calculating BMI from height and weight
#   - Deriving "training phase" from week number
#   - Computing estimated 1RM from weight and reps
#   - Adding "day of week" from a date column
# At WellMed, this might be calculating "days since last visit" or
# "risk score" from patient vitals. The key is: these fields should
# be deterministic (same input = same output) and documented.
"""

import uuid
from datetime import datetime, timezone

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.transformation.enricher")


class DataEnricher:
    """
    Adds derived columns and computed fields to Silver data.

    Usage:
        enricher = DataEnricher()
        enriched_df = enricher.enrich(df, dataset="workouts")
    """

    def enrich(self, df: pd.DataFrame, dataset: str) -> pd.DataFrame:
        """
        Route to the appropriate enrichment based on dataset.

        Args:
            df: Transformed DataFrame from the transformer
            dataset: Dataset identifier

        Returns:
            Enriched DataFrame with additional computed columns
        """
        enrichment_methods = {
            "exercises": self.enrich_exercises,
            "workouts": self.enrich_workouts,
            "body_metrics": self.enrich_body_metrics,
            "nutrition": self.enrich_nutrition,
        }

        method = enrichment_methods.get(dataset)
        if method is None:
            return self._add_common_fields(df)

        logger.info(
            f"Enriching {dataset}: {len(df)} rows",
            extra={"layer": "silver"},
        )
        result = method(df)
        return self._add_common_fields(result)

    @staticmethod
    def _add_common_fields(df: pd.DataFrame) -> pd.DataFrame:
        """Add common enrichment fields to all datasets."""
        df = df.copy()

        # Add a unique ID if not present
        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Add processing timestamp
        df["_processed_at"] = datetime.now(timezone.utc).isoformat()

        return df

    @staticmethod
    def enrich_exercises(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich exercise data with computed fields.

        Adds:
          - compound_score: How many muscle groups an exercise targets
          - popularity_tier: Categorize by usage (if usage data available)
        """
        df = df.copy()

        # Compound score: 1 (isolation) to 5 (full body compound)
        if "secondary_muscles" in df.columns:
            df["compound_score"] = df["secondary_muscles"].apply(lambda x: 1 + len(x) if isinstance(x, list) else 1)
        else:
            df["compound_score"] = 1

        # Exercise category grouping
        if "primary_muscle" in df.columns:
            muscle_group_map = {
                "chest": "upper_body_push",
                "shoulders": "upper_body_push",
                "triceps": "upper_body_push",
                "back": "upper_body_pull",
                "lats": "upper_body_pull",
                "biceps": "upper_body_pull",
                "traps": "upper_body_pull",
                "quads": "lower_body",
                "hamstrings": "lower_body",
                "glutes": "lower_body",
                "calves": "lower_body",
                "abs": "core",
                "obliques": "core",
                "full_body": "full_body",
            }
            df["movement_category"] = df["primary_muscle"].map(muscle_group_map).fillna("other")

        return df

    @staticmethod
    def enrich_workouts(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich workout data with computed training metrics.

        Adds:
          - day_of_week: Name of the day
          - week_number: ISO week number
          - set_volume_kg: Per-set volume (weight x reps)
          - estimated_1rm: Epley formula (weight x (1 + reps/30))
          - intensity_zone: RPE-based zone (warm-up / working / max effort)
          - volume_per_set: Average volume per working set (session-level)
          - training_phase: Inferred from week progression
        """
        df = df.copy()

        # Day of week from date
        if "workout_date" in df.columns:
            dates = pd.to_datetime(df["workout_date"], errors="coerce")
            df["day_of_week"] = dates.dt.day_name()
            df["week_number"] = dates.dt.isocalendar().week.astype("Int64")
            df["month"] = dates.dt.month

        # ── Per-set calculations ──
        # LEARN: Estimated 1RM (Epley formula) lets us track strength progress
        # even when rep ranges change. A lifter squatting 100kg x 8 and 120kg x 3
        # can be compared: e1RM = 100*(1+8/30) = 126.7 vs 120*(1+3/30) = 132.0.
        if "weight" in df.columns and "reps" in df.columns:
            weight = pd.to_numeric(df["weight"], errors="coerce")
            reps = pd.to_numeric(df["reps"], errors="coerce")
            df["set_volume_kg"] = (weight * reps).round(1)
            df["estimated_1rm"] = (weight * (1 + reps / 30)).round(1)

        # RPE-based intensity zones
        if "rpe" in df.columns:
            rpe = pd.to_numeric(df["rpe"], errors="coerce")
            df["intensity_zone"] = pd.cut(
                rpe,
                bins=[0, 7.0, 8.0, 9.0, 10.0],
                labels=["warm_up", "working", "hard", "max_effort"],
                right=True,
            )

        # Volume per set (if session-level volume and set data available)
        if "total_volume_kg" in df.columns and "total_sets" in df.columns:
            df["volume_per_set"] = (
                pd.to_numeric(df["total_volume_kg"], errors="coerce")
                / pd.to_numeric(df["total_sets"], errors="coerce").replace(0, None)
            ).round(1)

        # Duration category
        if "duration_minutes" in df.columns:
            duration = pd.to_numeric(df["duration_minutes"], errors="coerce")
            df["duration_category"] = pd.cut(
                duration,
                bins=[0, 45, 60, 75, 90, 300],
                labels=["short", "standard", "moderate", "long", "marathon"],
                right=True,
            )

        return df

    @staticmethod
    def enrich_body_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich body metrics with calculated health indicators.

        Adds:
          - bmi: Body Mass Index (weight / height^2)
          - fat_mass_kg: weight * body_fat_pct / 100
          - lean_mass_calc: weight - fat_mass
          - weight_change: Difference from previous measurement

        # LEARN: BMI = weight(kg) / height(m)^2
        # It's a rough health indicator. Not perfect (doesn't account for
        # muscle mass), but widely used in healthcare and insurance.
        """
        df = df.copy()

        # Fat mass calculation
        if "weight_kg" in df.columns and "body_fat_pct" in df.columns:
            weight = pd.to_numeric(df["weight_kg"], errors="coerce")
            bf_pct = pd.to_numeric(df["body_fat_pct"], errors="coerce")
            df["fat_mass_kg"] = (weight * bf_pct / 100).round(1)
            df["lean_mass_calc_kg"] = (weight - df["fat_mass_kg"]).round(1)

        # Weight trend (day-over-day change)
        if "weight_kg" in df.columns:
            df = df.sort_values("measured_at" if "measured_at" in df.columns else df.columns[0])
            weight = pd.to_numeric(df["weight_kg"], errors="coerce")
            df["weight_change_kg"] = weight.diff().round(2)

        # Recovery index (composite of sleep + stress + recovery)
        recovery_cols = ["sleep_quality", "stress_level", "recovery_score"]
        available = [c for c in recovery_cols if c in df.columns]
        if len(available) >= 2:
            for col in available:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            # Normalize stress (invert: low stress = high recovery)
            if "stress_level" in df.columns:
                df["_stress_inv"] = 10 - df["stress_level"]
                available_for_calc = [c if c != "stress_level" else "_stress_inv" for c in available]
            else:
                available_for_calc = available
            # Simple average as recovery index
            df["recovery_index"] = df[available_for_calc].mean(axis=1).round(1)
            if "_stress_inv" in df.columns:
                df = df.drop(columns=["_stress_inv"])

        return df

    @staticmethod
    def enrich_nutrition(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich nutrition data with macro ratios and daily totals.

        Adds:
          - total_macros_g: protein + carbs + fats
          - protein_pct / carbs_pct / fats_pct: Macro split percentages
          - calories_from_protein/carbs/fats: Individual calorie contributions
        """
        df = df.copy()

        protein = (
            pd.to_numeric(df["protein_g"], errors="coerce").fillna(0)
            if "protein_g" in df.columns
            else pd.Series(0, index=df.index)
        )
        carbs = (
            pd.to_numeric(df["carbs_g"], errors="coerce").fillna(0)
            if "carbs_g" in df.columns
            else pd.Series(0, index=df.index)
        )
        fats = (
            pd.to_numeric(df["fats_g"], errors="coerce").fillna(0)
            if "fats_g" in df.columns
            else pd.Series(0, index=df.index)
        )

        # Calorie contributions per macro
        # Protein: 4 cal/g, Carbs: 4 cal/g, Fats: 9 cal/g
        df["calories_from_protein"] = (protein * 4).round(0)
        df["calories_from_carbs"] = (carbs * 4).round(0)
        df["calories_from_fats"] = (fats * 9).round(0)

        total_cal = df["calories_from_protein"] + df["calories_from_carbs"] + df["calories_from_fats"]

        # Macro split percentages
        df["protein_pct"] = (df["calories_from_protein"] / total_cal.replace(0, None) * 100).round(1)
        df["carbs_pct"] = (df["calories_from_carbs"] / total_cal.replace(0, None) * 100).round(1)
        df["fats_pct"] = (df["calories_from_fats"] / total_cal.replace(0, None) * 100).round(1)

        df["total_macros_g"] = (protein + carbs + fats).round(1)

        return df
