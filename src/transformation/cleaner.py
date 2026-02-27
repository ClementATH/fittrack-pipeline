"""
Data Cleaner (Silver Layer — Step 1)
=====================================

WHAT: Applies standardized cleaning operations to raw Bronze data:
  - Column name standardization (to snake_case)
  - Data type casting (strings to dates, ints, etc.)
  - Null handling (fill, drop, or flag)
  - Deduplication (remove exact and near duplicates)
  - Whitespace trimming and string normalization

WHY: Raw data from APIs and files is messy. Column names are inconsistent
("userName", "User Name", "user_name"), types are wrong (numbers stored
as strings), and duplicates sneak in from multiple ingestion runs.
Cleaning is the first transformation step — everything downstream
depends on clean, consistent data.

# LEARN: The cleaning step is where 60-80% of data engineering time goes.
# It's not glamorous, but it's the foundation. At WellMed, you've probably
# seen patient records with "M", "Male", "MALE", "male" in the same column.
# A cleaner standardizes all of these to one value. Without this step,
# every downstream query has to handle all variations — which means
# bugs, inconsistencies, and wrong numbers on dashboards.

What Would Break If you skipped cleaning:
  - JOIN operations would fail on mismatched column names
  - Aggregations would give wrong results (duplicates = double counting)
  - Date filters wouldn't work on string columns
  - Dashboards would show "null" everywhere
"""

import re
from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.transformation.cleaner")


class DataCleaner:
    """
    Applies standardized cleaning operations to raw DataFrames.

    Cleaning is applied in a specific order to prevent conflicts:
    1. Column name standardization
    2. Strip whitespace from strings
    3. Handle null values
    4. Cast data types
    5. Remove duplicates

    Usage:
        cleaner = DataCleaner()
        clean_df = cleaner.clean(raw_df, table_name="exercises")
    """

    def __init__(self, naming_convention: str = "snake_case"):
        self.naming_convention = naming_convention

    @staticmethod
    def to_snake_case(name: str) -> str:
        """
        Convert any string to snake_case.

        Examples:
            "userName"      -> "user_name"
            "User Name"     -> "user_name"
            "user-name"     -> "user_name"
            "UserID"        -> "user_id"
            "HTTPResponse"  -> "http_response"
            "already_snake" -> "already_snake"

        # LEARN: snake_case is the Python and SQL standard for column names.
        # CamelCase is for classes, snake_case is for variables and columns.
        # Standardizing this means your team never has to guess whether
        # to write "userId", "user_id", or "UserId" in a query.
        """
        # Handle empty strings
        if not name:
            return name

        # Replace common separators with underscore
        name = re.sub(r"[\s\-\.\+]+", "_", name)

        # Insert underscore before uppercase letters that follow lowercase
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)

        # Insert underscore between consecutive uppercase and following lowercase
        name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)

        # Lowercase everything and clean up multiple underscores
        name = name.lower().strip("_")
        name = re.sub(r"_+", "_", name)

        return name

    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize all column names to snake_case.

        Also removes any leading/trailing underscores and special characters
        that would break SQL queries.
        """
        original_cols = df.columns.tolist()
        new_cols = [self.to_snake_case(col) for col in original_cols]

        # Handle duplicate column names after transformation
        seen: dict[str, int] = {}
        unique_cols: list[str] = []
        for col in new_cols:
            if col in seen:
                seen[col] += 1
                unique_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_cols.append(col)

        df = df.copy()
        df.columns = unique_cols

        # Log renamed columns
        renamed = {
            old: new for old, new in zip(original_cols, unique_cols) if old != new
        }
        if renamed:
            logger.debug(
                f"Renamed {len(renamed)} columns: "
                f"{list(renamed.items())[:5]}{'...' if len(renamed) > 5 else ''}"
            )

        return df

    @staticmethod
    def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
        """
        Strip whitespace from all string columns.

        # LEARN: Leading/trailing whitespace is invisible but causes
        # JOIN failures. "Marcus" != "Marcus " in SQL. This single
        # operation prevents countless debugging headaches.
        """
        df = df.copy()
        string_cols = df.select_dtypes(include=["object"]).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)
        return df

    @staticmethod
    def handle_nulls(
        df: pd.DataFrame,
        numeric_fill: float | None = None,
        string_fill: str | None = None,
    ) -> pd.DataFrame:
        """
        Handle null values based on column type.

        Args:
            df: Input DataFrame
            numeric_fill: Value to fill nulls in numeric columns (None = keep nulls)
            string_fill: Value to fill nulls in string columns (None = keep nulls)

        # LEARN: How you handle nulls depends on the business context.
        # For body weight: null means "not measured" — keep it null
        # For calorie count: null might mean 0 — fill it
        # For exercise name: null is invalid — flag/reject it
        # Never blindly fill all nulls with 0 — that changes the meaning of your data.
        """
        df = df.copy()

        if numeric_fill is not None:
            numeric_cols = df.select_dtypes(include=["number"]).columns
            df[numeric_cols] = df[numeric_cols].fillna(numeric_fill)

        if string_fill is not None:
            string_cols = df.select_dtypes(include=["object"]).columns
            df[string_cols] = df[string_cols].fillna(string_fill)

        return df

    @staticmethod
    def cast_types(
        df: pd.DataFrame,
        type_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """
        Cast columns to specified data types.

        Args:
            type_map: Dictionary of {column_name: target_type}
                      Types: "string", "integer", "float", "date", "datetime", "boolean"
        """
        if not type_map:
            return df

        df = df.copy()
        for col, target_type in type_map.items():
            if col not in df.columns:
                continue

            try:
                if target_type in ("string", "str"):
                    df[col] = df[col].astype(str).replace("None", None).replace("nan", None)
                elif target_type in ("integer", "int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif target_type in ("float", "numeric", "decimal"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif target_type == "date":
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                elif target_type == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif target_type in ("boolean", "bool"):
                    df[col] = df[col].astype(bool)
                else:
                    logger.warning(f"Unknown type '{target_type}' for column '{col}'")
            except Exception as e:
                logger.warning(f"Failed to cast {col} to {target_type}: {e}")

        return df

    @staticmethod
    def deduplicate(
        df: pd.DataFrame,
        subset: list[str] | None = None,
        keep: str = "last",
    ) -> pd.DataFrame:
        """
        Remove duplicate rows.

        Args:
            df: Input DataFrame
            subset: Columns to consider for deduplication. If None, uses all columns
                    except metadata columns (starting with '_').
            keep: Which duplicate to keep: 'first', 'last', or False (drop all dupes)

        # LEARN: Deduplication is critical when ingesting incrementally.
        # If the pipeline runs twice on the same data (retry after failure),
        # you'd get double records without dedup. The 'keep' strategy matters:
        #   - 'last': Keep the most recent version (good for updates)
        #   - 'first': Keep the original (good for immutable records)
        """
        original_count = len(df)

        if subset is None:
            # Exclude metadata columns for dedup comparison
            subset = [col for col in df.columns if not col.startswith("_")]

        df = df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)

        removed = original_count - len(df)
        if removed > 0:
            logger.info(f"Removed {removed} duplicate rows (kept {keep})")

        return df

    def clean(
        self,
        df: pd.DataFrame,
        table_name: str = "unknown",
        type_map: dict[str, str] | None = None,
        dedup_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Apply the full cleaning pipeline to a DataFrame.

        This is the main method — applies all cleaning steps in order.

        Args:
            df: Raw DataFrame from Bronze layer
            table_name: Name of the dataset (for logging)
            type_map: Column -> type mapping for casting
            dedup_columns: Columns to use for deduplication

        Returns:
            Cleaned DataFrame ready for further transformation
        """
        logger.info(
            f"Cleaning {table_name}: {len(df)} rows, {len(df.columns)} columns",
            extra={"layer": "silver"},
        )

        # Step 1: Standardize column names
        df = self.standardize_columns(df)

        # Step 2: Trim whitespace
        df = self.trim_strings(df)

        # Step 3: Handle nulls (keep nulls by default — don't fill blindly)
        df = self.handle_nulls(df)

        # Step 4: Cast types if mapping provided
        if type_map:
            df = self.cast_types(df, type_map)

        # Step 5: Deduplicate
        df = self.deduplicate(df, subset=dedup_columns)

        logger.info(
            f"Cleaning complete for {table_name}: "
            f"{len(df)} rows remaining",
            extra={"layer": "silver"},
        )
        return df
