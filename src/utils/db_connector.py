"""
Database Connector (DuckDB)
============================

WHAT: Manages connections to DuckDB, the local analytical database that
serves as our data warehouse for this project.

WHY: DuckDB was chosen because:
  1. Zero infrastructure — no server to install or manage (unlike Postgres/MySQL)
  2. Columnar storage — optimized for analytical queries (like Snowflake)
  3. Reads/writes Parquet natively — our pipeline stores data as Parquet files
  4. SQL-compatible — the SQL you write here transfers to Snowflake/BigQuery
  5. Embedded — runs inside Python, perfect for local development

HOW: Provides a connection manager (context manager pattern) and helper
methods for common operations (execute, query, load Parquet, etc.).

# LEARN: DuckDB is to analytical databases what SQLite is to transactional
# databases. At WellMed, you use Snowflake (cloud columnar DB). DuckDB
# gives you the same columnar query patterns locally for development
# and testing. The SQL you write here will work almost identically
# in Snowflake — that's the beauty of SQL standards.

What Would Break If you didn't use a connection manager:
  - Connections could leak (stay open forever), exhausting system resources
  - Concurrent access could corrupt the database file
  - Errors during queries would leave connections in a bad state
"""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("fittrack.db")


class DuckDBConnector:
    """
    Manages DuckDB database connections and operations.

    Usage:
        db = DuckDBConnector("data/fittrack.duckdb")

        # Using context manager (recommended)
        with db.connection() as conn:
            result = conn.execute("SELECT * FROM exercises").fetchdf()

        # Using helper methods
        df = db.query("SELECT * FROM exercises WHERE difficulty = ?", ["beginner"])
    """

    def __init__(self, db_path: str = "data/fittrack.duckdb", read_only: bool = False):
        """
        Initialize the DuckDB connector.

        Args:
            db_path: Path to the DuckDB database file. Created if it doesn't exist.
            read_only: Open in read-only mode (for concurrent reads).

        # LEARN: DuckDB stores everything in a single file (like SQLite).
        # In production, you'd use Snowflake/BigQuery instead, but the
        # query patterns are identical.
        """
        self.db_path = Path(db_path)
        self.read_only = read_only

        # Ensure the parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"DuckDB connector initialized: {self.db_path}")

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Context manager that provides a DuckDB connection.

        # LEARN: Context managers (the 'with' statement) guarantee cleanup.
        # Even if your code throws an error mid-query, the connection
        # will be properly closed. This is the same pattern used for
        # file handles, database connections, and network sockets
        # everywhere in professional Python code.

        Usage:
            with db.connection() as conn:
                conn.execute("CREATE TABLE ...")
                result = conn.execute("SELECT ...").fetchdf()
        """
        conn = duckdb.connect(str(self.db_path), read_only=self.read_only)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        """Execute a SQL statement (CREATE, INSERT, UPDATE, DELETE)."""
        with self.connection() as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
            logger.debug(f"Executed SQL: {sql[:100]}...")

    def query(self, sql: str, params: list[Any] | None = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as a Pandas DataFrame.

        # LEARN: Returning DataFrames is the standard pattern for analytical
        # queries. DataFrames give you vectorized operations, which are
        # orders of magnitude faster than row-by-row Python loops.
        """
        with self.connection() as conn:
            if params:
                result = conn.execute(sql, params).fetchdf()
            else:
                result = conn.execute(sql).fetchdf()
            logger.debug(f"Query returned {len(result)} rows: {sql[:80]}...")
            return result

    def load_parquet(self, table_name: str, parquet_path: str | Path) -> int:
        """
        Load a Parquet file directly into a DuckDB table.

        # LEARN: DuckDB reads Parquet files natively without loading them
        # into memory first. This is extremely efficient for large datasets.
        # Snowflake has the same capability with its COPY INTO command.

        Args:
            table_name: Target table name in DuckDB
            parquet_path: Path to the Parquet file

        Returns:
            Number of rows loaded
        """
        parquet_path = Path(parquet_path)
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        with self.connection() as conn:
            # CREATE OR REPLACE TABLE ... AS SELECT * FROM parquet file
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS " f"SELECT * FROM read_parquet('{parquet_path}')")
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {count} rows from {parquet_path} into {table_name}")
            return count

    def load_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        mode: str = "replace",
    ) -> int:
        """
        Load a Pandas DataFrame into a DuckDB table.

        Args:
            table_name: Target table name
            df: DataFrame to load
            mode: 'replace' (drop and recreate) or 'append'

        Returns:
            Number of rows loaded
        """
        with self.connection() as conn:
            if mode == "replace":
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            elif mode == "append":
                # Try to insert; create table if it doesn't exist
                try:
                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                except duckdb.CatalogException:
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            else:
                raise ValueError(f"Unknown mode: {mode}. Use 'replace' or 'append'.")

            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {len(df)} rows into {table_name} (mode={mode})")
            return count

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables " "WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result[0] > 0

    def get_table_row_count(self, table_name: str) -> int:
        """Get the row count of a table."""
        with self.connection() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0]

    def get_tables(self) -> list[str]:
        """List all tables in the database."""
        with self.connection() as conn:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables " "WHERE table_schema = 'main'"
            ).fetchdf()
            return result["table_name"].tolist()

    def export_to_parquet(self, table_name: str, output_path: str | Path) -> Path:
        """
        Export a DuckDB table to a Parquet file.

        # LEARN: Parquet is the standard file format for data engineering.
        # It's columnar (reads only needed columns), compressed (small files),
        # and typed (preserves data types). At WellMed with Snowflake,
        # you'd use Parquet for data lake storage on S3.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with self.connection() as conn:
            conn.execute(f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)")
        logger.info(f"Exported {table_name} to {output_path}")
        return output_path

    def init_warehouse_schema(self) -> None:
        """
        Initialize the Gold layer warehouse schema in DuckDB,
        mirroring the existing FitTrack Pro Postgres schema.

        # LEARN: We're creating DuckDB tables that mirror the Postgres
        # schema from 01_schema.sql. This lets us develop and test locally
        # without needing a Supabase/Postgres instance running.
        """
        with self.connection() as conn:
            # Exercises table (Gold layer target for Wger API data)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_exercises (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    slug VARCHAR UNIQUE,
                    primary_muscle VARCHAR NOT NULL,
                    secondary_muscles VARCHAR[],
                    exercise_type VARCHAR NOT NULL,
                    equipment VARCHAR NOT NULL,
                    difficulty VARCHAR DEFAULT 'intermediate',
                    instructions VARCHAR,
                    tips VARCHAR[],
                    video_url VARCHAR,
                    is_unilateral BOOLEAN DEFAULT FALSE,
                    is_custom BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    -- SCD Type 2 fields
                    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP DEFAULT '9999-12-31',
                    is_current BOOLEAN DEFAULT TRUE
                )
            """)

            # Athletes table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_athletes (
                    id VARCHAR PRIMARY KEY,
                    email VARCHAR UNIQUE,
                    username VARCHAR UNIQUE,
                    full_name VARCHAR NOT NULL,
                    date_of_birth DATE,
                    gender VARCHAR,
                    height_cm DECIMAL(5,1),
                    target_weight_kg DECIMAL(5,1),
                    activity_level VARCHAR,
                    training_experience_years INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    effective_to TIMESTAMP DEFAULT '9999-12-31',
                    is_current BOOLEAN DEFAULT TRUE
                )
            """)

            # Workouts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_workouts (
                    id VARCHAR PRIMARY KEY,
                    athlete_id VARCHAR NOT NULL,
                    program_id VARCHAR,
                    workout_date DATE NOT NULL,
                    day_name VARCHAR,
                    status VARCHAR NOT NULL DEFAULT 'completed',
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_minutes INTEGER,
                    total_volume_kg DECIMAL(10,2),
                    total_sets INTEGER,
                    energy_level INTEGER,
                    pump_rating INTEGER,
                    sleep_hours_prior DECIMAL(3,1),
                    notes VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Body Metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_body_metrics (
                    id VARCHAR PRIMARY KEY,
                    athlete_id VARCHAR NOT NULL,
                    measured_at DATE NOT NULL,
                    weight_kg DECIMAL(5,2),
                    body_fat_pct DECIMAL(4,1),
                    lean_mass_kg DECIMAL(5,2),
                    waist_cm DECIMAL(5,1),
                    chest_cm DECIMAL(5,1),
                    resting_heart_rate INTEGER,
                    sleep_quality INTEGER,
                    stress_level INTEGER,
                    recovery_score INTEGER,
                    steps INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Nutrition Logs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_nutrition_logs (
                    id VARCHAR PRIMARY KEY,
                    athlete_id VARCHAR NOT NULL,
                    log_date DATE NOT NULL,
                    meal_type VARCHAR NOT NULL,
                    meal_name VARCHAR,
                    calories INTEGER NOT NULL,
                    protein_g DECIMAL(6,1),
                    carbs_g DECIMAL(6,1),
                    fats_g DECIMAL(6,1),
                    fiber_g DECIMAL(5,1),
                    sodium_mg INTEGER,
                    sugar_g DECIMAL(5,1),
                    water_ml INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Pipeline metadata table (for tracking runs)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR PRIMARY KEY,
                    pipeline_name VARCHAR NOT NULL,
                    source_name VARCHAR,
                    layer VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    rows_processed INTEGER DEFAULT 0,
                    rows_failed INTEGER DEFAULT 0,
                    error_message VARCHAR,
                    metadata JSON
                )
            """)

            # Data quality scores table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_scores (
                    id VARCHAR PRIMARY KEY,
                    table_name VARCHAR NOT NULL,
                    run_id VARCHAR,
                    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    overall_score DECIMAL(5,2),
                    completeness_score DECIMAL(5,2),
                    accuracy_score DECIMAL(5,2),
                    consistency_score DECIMAL(5,2),
                    timeliness_score DECIMAL(5,2),
                    row_count INTEGER,
                    failed_checks INTEGER DEFAULT 0,
                    details JSON
                )
            """)

        logger.info("Warehouse schema initialized in DuckDB")
