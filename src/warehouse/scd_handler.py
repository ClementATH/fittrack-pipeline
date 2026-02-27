"""
Slowly Changing Dimension Handler (Gold Layer)
=================================================

WHAT: Implements SCD Type 2 logic for dimension tables that change over time.

WHY: In the real world, dimension attributes change:
  - An athlete changes their training goal (hypertrophy -> strength)
  - An exercise gets reclassified (isolation -> compound)
  - A food's nutritional profile gets updated

SCD Type 2 tracks the FULL HISTORY of these changes by:
  1. Closing the old record (set effective_to = now, is_current = false)
  2. Inserting a new record (effective_from = now, is_current = true)
  3. Both records exist — historical queries use effective dates

# LEARN: This is one of the most important patterns in dimensional modeling.
# You'll see this in every enterprise data warehouse. Master this and you're
# ahead of 80% of junior DEs.
#
# There are three types of SCDs:
#   Type 1: Overwrite the old value (no history — DON'T do this for important data)
#   Type 2: Add a new row with dates (full history — this is what we implement)
#   Type 3: Add a column for the old value (limited history — rarely used)
#
# At WellMed, patient address changes are a classic SCD Type 2 scenario.
# If Marcus moves from Austin to Dallas, you need to know he was in Austin
# for claims filed BEFORE the move and Dallas for claims filed AFTER.
#
# What Would Break If you used SCD Type 1 (overwrite):
#   - Historical reports would show wrong data
#   - "Where did this patient live when they filed claim X?" — no answer
#   - Regulatory audits would fail (healthcare requires full history)
#   - Time-based analytics would be incorrect

Example:
  Before update:
    | id   | name          | goal         | effective_from | effective_to | is_current |
    | abc  | Marcus Chen   | hypertrophy  | 2025-01-01     | 9999-12-31   | true       |

  After Marcus changes goal to "strength":
    | id   | name          | goal         | effective_from | effective_to | is_current |
    | abc  | Marcus Chen   | hypertrophy  | 2025-01-01     | 2025-06-15   | false      |
    | def  | Marcus Chen   | strength     | 2025-06-15     | 9999-12-31   | true       |
"""

import uuid
from datetime import datetime, timezone

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.warehouse.scd")


def apply_scd_type2(
    existing_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    key_columns: list[str],
    tracked_columns: list[str],
    effective_from_col: str = "effective_from",
    effective_to_col: str = "effective_to",
    current_flag_col: str = "is_current",
) -> pd.DataFrame:
    """
    Apply Slowly Changing Dimension Type 2 logic.

    WHAT: Tracks historical changes to dimension records by closing old
    records and inserting new ones, rather than overwriting.

    WHY: In a data warehouse, we often need to know what a record looked
    like at a specific point in time. SCD Type 2 preserves that history.

    Example: If a customer moves from Austin to Dallas, we keep BOTH
    records with effective dates so reports can show the right city
    for the right time period.

    HOW:
    1. Compare incoming records against existing records on key columns
    2. If tracked columns changed -> close the old record (set end_date)
    3. Insert the new version with a fresh start_date
    4. Unchanged records are left alone

    Args:
        existing_df: Current dimension table (may be empty for first load)
        incoming_df: New data to merge in
        key_columns: Business key columns that identify a unique record
            (e.g., ['slug'] for exercises, ['email'] for athletes)
        tracked_columns: Columns to monitor for changes
            (e.g., ['primary_muscle', 'difficulty'] for exercises)
        effective_from_col: Column name for record start date
        effective_to_col: Column name for record end date
        current_flag_col: Column name for "is this the current version?" flag

    Returns:
        Updated DataFrame with full SCD Type 2 history

    # LEARN: This is one of the most important patterns in dimensional modeling.
    # You'll see this in every enterprise data warehouse. Master this and you're
    # ahead of 80% of junior DEs.
    """
    now = datetime.now(timezone.utc).isoformat()
    far_future = "9999-12-31T00:00:00+00:00"

    # Handle first-time load (no existing data)
    if existing_df is None or existing_df.empty:
        logger.info(
            f"First-time load: inserting {len(incoming_df)} records",
            extra={"layer": "gold"},
        )
        result = incoming_df.copy()
        if "id" not in result.columns:
            result["id"] = [str(uuid.uuid4()) for _ in range(len(result))]
        result[effective_from_col] = now
        result[effective_to_col] = far_future
        result[current_flag_col] = True
        return result

    # Get current records from existing dimension
    current_mask = existing_df[current_flag_col] == True  # noqa: E712
    current_records = existing_df[current_mask].copy()
    historical_records = existing_df[~current_mask].copy()

    # Stats tracking
    unchanged = 0
    updated = 0
    new_records = 0

    # Records to add (new versions of changed records + brand new records)
    records_to_add: list[dict] = []
    # Records to close (old versions of changed records)
    records_to_close: list[int] = []  # indices in current_records

    for _, incoming_row in incoming_df.iterrows():
        # Find matching existing record by business key
        match_mask = pd.Series([True] * len(current_records))
        for key_col in key_columns:
            if key_col in current_records.columns and key_col in incoming_row.index:
                match_mask = match_mask & (
                    current_records[key_col] == incoming_row[key_col]
                )

        matches = current_records[match_mask]

        if matches.empty:
            # Brand new record — insert with current effective dates
            new_row = incoming_row.to_dict()
            new_row["id"] = str(uuid.uuid4())
            new_row[effective_from_col] = now
            new_row[effective_to_col] = far_future
            new_row[current_flag_col] = True
            records_to_add.append(new_row)
            new_records += 1
        else:
            # Existing record found — check if tracked columns changed
            existing_row = matches.iloc[0]
            has_changed = False

            for tracked_col in tracked_columns:
                if tracked_col not in existing_row.index or tracked_col not in incoming_row.index:
                    continue
                old_val = existing_row.get(tracked_col)
                new_val = incoming_row.get(tracked_col)
                # Handle NaN comparison
                if pd.isna(old_val) and pd.isna(new_val):
                    continue
                if str(old_val) != str(new_val):
                    has_changed = True
                    break

            if has_changed:
                # Close the old record
                idx = matches.index[0]
                records_to_close.append(idx)

                # Create new version
                new_row = incoming_row.to_dict()
                new_row["id"] = str(uuid.uuid4())
                new_row[effective_from_col] = now
                new_row[effective_to_col] = far_future
                new_row[current_flag_col] = True
                records_to_add.append(new_row)
                updated += 1
            else:
                unchanged += 1

    # Apply closures to current records
    for idx in records_to_close:
        current_records.at[idx, effective_to_col] = now
        current_records.at[idx, current_flag_col] = False

    # Build the final result: historical + updated current + new records
    parts = [historical_records, current_records]
    if records_to_add:
        new_df = pd.DataFrame(records_to_add)
        parts.append(new_df)

    result = pd.concat(parts, ignore_index=True)

    logger.info(
        f"SCD Type 2 applied: {new_records} new, {updated} updated, "
        f"{unchanged} unchanged. Total rows: {len(result)}",
        extra={"layer": "gold"},
    )
    return result
