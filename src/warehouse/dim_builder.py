"""
Dimension Builder (Gold Layer)
===============================

WHAT: Builds dimension tables for the data warehouse from Silver layer data.
Dimensions describe the "who, what, where, when" of your business data.

WHY: Dimensional modeling (star schema) is the foundation of every data
warehouse. Dimension tables provide the context for your fact tables:
  - dim_exercises: WHAT exercises were performed
  - dim_athletes: WHO performed them
  - dim_date: WHEN they happened
  - dim_equipment: WHAT equipment was used
  - dim_muscle_groups: WHAT muscles were targeted

# LEARN: Dimension tables are the "lookup tables" of a data warehouse.
# When Marcus queries "show me my chest volume this month", the system:
#   1. Looks up "chest" in dim_muscle_groups (dimension)
#   2. Finds all matching records in fact_workout_sets (fact)
#   3. Sums the volume
# At WellMed with Snowflake, your patient demographics, provider info,
# and facility data are all dimension tables.

Star Schema Cheat Sheet:
  - Dimension = descriptive attributes (who, what, where, when)
  - Fact = measurable events (workout sets, body weigh-ins, meals)
  - Surrogate Key = auto-generated ID (independent of source system)
  - Natural Key = business identifier from the source system (email, slug)
"""

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logger import get_logger

logger = get_logger("fittrack.warehouse.dim")


class DimensionBuilder:
    """
    Builds and maintains dimension tables in the Gold layer.

    Usage:
        builder = DimensionBuilder(gold_path="data/gold")
        builder.build_dim_exercises(silver_df)
        builder.build_dim_date("2025-01-01", "2026-12-31")
    """

    def __init__(self, gold_path: str = "data/gold"):
        self.gold_path = Path(gold_path)
        self.gold_path.mkdir(parents=True, exist_ok=True)

    def _store_dimension(self, df: pd.DataFrame, dim_name: str) -> Path:
        """Store a dimension table as Parquet in the Gold layer."""
        file_path = self.gold_path / f"{dim_name}.parquet"
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")
        logger.info(
            f"Stored dimension {dim_name}: {len(df)} rows -> {file_path}",
            extra={"layer": "gold"},
        )
        return file_path

    def _load_dimension(self, dim_name: str) -> pd.DataFrame | None:
        """Load an existing dimension table if it exists."""
        file_path = self.gold_path / f"{dim_name}.parquet"
        if file_path.exists():
            return pd.read_parquet(file_path)
        return None

    def build_dim_exercises(self, silver_df: pd.DataFrame) -> pd.DataFrame:
        """
        Build the exercise dimension table from Silver data.

        Maps to the existing exercises table in 01_schema.sql.

        # LEARN: This dimension captures everything about an exercise:
        # its name, target muscles, equipment needed, difficulty, etc.
        # In a star schema, this table is referenced by fact_workout_sets
        # via a foreign key.
        """
        df = silver_df.copy()

        # Generate surrogate keys
        # LEARN: Surrogate keys are warehouse-generated IDs that are
        # independent of the source system. Why not use the source ID?
        # Because if you switch from Wger to another API, your warehouse
        # IDs stay stable. Facts reference surrogate keys, not source keys.
        if "id" not in df.columns:
            df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Ensure required columns exist with defaults
        defaults = {
            "name": "Unknown Exercise",
            "slug": "unknown",
            "primary_muscle": "full_body",
            "secondary_muscles": None,
            "exercise_type": "compound",
            "equipment": "none",
            "difficulty": "intermediate",
            "instructions": None,
            "is_unilateral": False,
            "is_custom": False,
        }

        for col, default_val in defaults.items():
            if col not in df.columns:
                df[col] = default_val

        # Add dimension metadata
        now = datetime.now(timezone.utc).isoformat()
        df["effective_from"] = now
        df["effective_to"] = "9999-12-31T00:00:00+00:00"
        df["is_current"] = True

        # Select final columns
        dim_cols = [*defaults.keys(), "id", "effective_from", "effective_to", "is_current"]
        # Add any enrichment columns that exist
        for col in ["compound_score", "movement_category"]:
            if col in df.columns:
                dim_cols.append(col)

        df = df[[c for c in dim_cols if c in df.columns]]

        self._store_dimension(df, "dim_exercises")
        logger.info(f"Built dim_exercises: {len(df)} exercises")
        return df

    def build_dim_athletes(self, silver_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """
        Build the athletes dimension table.

        If no Silver data provided, creates from existing seed data.
        Maps to athletes table in 01_schema.sql.
        """
        if silver_df is not None:
            df = silver_df.copy()
        else:
            # Create from our known seed data
            df = pd.DataFrame(
                [
                    {
                        "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                        "email": "marcus.chen@email.com",
                        "username": "marcuslifts",
                        "full_name": "Marcus Chen",
                        "date_of_birth": "1997-03-15",
                        "gender": "male",
                        "height_cm": 180.3,
                        "target_weight_kg": 81.6,
                        "activity_level": "very_active",
                        "training_experience_years": 6,
                    }
                ]
            )

        now = datetime.now(timezone.utc).isoformat()
        df["effective_from"] = now
        df["effective_to"] = "9999-12-31T00:00:00+00:00"
        df["is_current"] = True

        self._store_dimension(df, "dim_athletes")
        logger.info(f"Built dim_athletes: {len(df)} athletes")
        return df

    def build_dim_date(
        self,
        start_date: str = "2025-01-01",
        end_date: str = "2026-12-31",
    ) -> pd.DataFrame:
        """
        Build a date dimension table.

        # LEARN: A date dimension is in EVERY data warehouse. Instead of
        # calculating "is this a weekend?" or "what quarter is this?" in
        # every query, you pre-compute these attributes once. Then queries
        # just JOIN to dim_date and filter on attributes.
        #
        # At WellMed, your Snowflake warehouse almost certainly has a
        # date dimension. It's used for time-series analysis, fiscal year
        # reporting, and holiday-aware calculations.
        #
        # Example query WITH dim_date:
        #   SELECT d.quarter, SUM(f.volume)
        #   FROM fact_workouts f JOIN dim_date d ON f.workout_date = d.date_key
        #   WHERE d.is_weekday = true
        #   GROUP BY d.quarter
        #
        # WITHOUT dim_date, every query recalculates:
        #   WHERE EXTRACT(DOW FROM workout_date) NOT IN (0, 6)
        """
        dates = pd.date_range(start=start_date, end=end_date, freq="D")

        df = pd.DataFrame(
            {
                "date_key": dates.strftime("%Y-%m-%d"),
                "full_date": dates,
                "year": dates.year,
                "quarter": dates.quarter,
                "month": dates.month,
                "month_name": dates.strftime("%B"),
                "week_of_year": dates.isocalendar().week.astype(int),
                "day_of_month": dates.day,
                "day_of_week": dates.dayofweek,  # 0=Monday, 6=Sunday
                "day_name": dates.strftime("%A"),
                "is_weekday": dates.dayofweek < 5,
                "is_weekend": dates.dayofweek >= 5,
                "is_month_start": dates.is_month_start,
                "is_month_end": dates.is_month_end,
                "fiscal_year": dates.year,  # Customize for your org's fiscal year
                "fiscal_quarter": dates.quarter,
            }
        )

        self._store_dimension(df, "dim_date")
        logger.info(f"Built dim_date: {len(df)} days ({start_date} to {end_date})")
        return df

    def build_dim_muscle_groups(self) -> pd.DataFrame:
        """Build muscle group dimension from the enum values in 01_schema.sql."""
        muscle_groups = [
            {"id": str(uuid.uuid4()), "name": m, "body_region": r, "is_compound_target": c}
            for m, r, c in [
                ("chest", "upper_body", True),
                ("back", "upper_body", True),
                ("shoulders", "upper_body", True),
                ("biceps", "upper_body", False),
                ("triceps", "upper_body", False),
                ("forearms", "upper_body", False),
                ("quads", "lower_body", True),
                ("hamstrings", "lower_body", True),
                ("glutes", "lower_body", True),
                ("calves", "lower_body", False),
                ("abs", "core", False),
                ("obliques", "core", False),
                ("traps", "upper_body", False),
                ("lats", "upper_body", True),
                ("hip_flexors", "lower_body", False),
                ("adductors", "lower_body", False),
                ("abductors", "lower_body", False),
                ("neck", "upper_body", False),
                ("full_body", "full_body", True),
            ]
        ]
        df = pd.DataFrame(muscle_groups)
        self._store_dimension(df, "dim_muscle_groups")
        logger.info(f"Built dim_muscle_groups: {len(df)} muscle groups")
        return df
