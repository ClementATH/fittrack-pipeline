"""
Base Ingestor (Abstract Class)
===============================

WHAT: Defines the contract that ALL ingestors must follow. Whether you're
pulling data from an API, reading CSV files, or connecting to a database,
every ingestor implements the same interface.

WHY: The "Strategy Pattern" — by defining a common interface, the pipeline
orchestrator doesn't need to know HOW data is ingested. It just calls
ingestor.ingest() and gets a DataFrame back. This means adding a new
data source (say, a Kafka stream) only requires writing ONE new class,
not changing any existing code.

# LEARN: This is the "Open/Closed Principle" from SOLID design:
# Open for extension (add new ingestors), Closed for modification
# (existing code doesn't change). This is exactly how data platforms
# at companies like WellMed are built — a base class defines the
# contract, and each data source gets its own implementation.

What Would Break If you didn't have a base class:
  - Every ingestor would have different method names and signatures
  - The orchestrator would need if/else chains for each source type
  - Adding a new source would require changing the orchestrator
  - Testing would require different approaches for each ingestor
"""

import hashlib
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config_loader import SourceConfig
from src.utils.logger import get_logger

logger = get_logger("fittrack.ingestion")


class BaseIngestor(ABC):
    """
    Abstract base class for all data ingestors.

    Every ingestor must implement:
      - extract(): Pull raw data from the source
      - ingest(): Full ingestion flow (extract + add metadata + store)

    The base class provides:
      - Metadata enrichment (batch_id, timestamps, source hash)
      - Parquet storage with consistent naming
      - Idempotency via content hashing
    """

    def __init__(
        self,
        source_name: str,
        source_config: SourceConfig,
        bronze_path: str = "data/bronze",
    ):
        """
        Args:
            source_name: Identifier for this source (e.g., 'wger_exercises')
            source_config: Configuration from sources.yaml
            bronze_path: Where to store raw ingested data
        """
        self.source_name = source_name
        self.config = source_config
        self.bronze_path = Path(bronze_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def extract(self, endpoint_name: str | None = None, **kwargs: Any) -> pd.DataFrame:
        """
        Extract raw data from the source.

        This is the method each concrete ingestor must implement.
        It should return the raw data as a Pandas DataFrame.

        Args:
            endpoint_name: For API sources, which endpoint to call
            **kwargs: Additional extraction parameters

        Returns:
            Raw data as a Pandas DataFrame
        """
        ...

    def add_metadata(self, df: pd.DataFrame, batch_id: str | None = None) -> pd.DataFrame:
        """
        Add ingestion metadata columns to the raw data.

        # LEARN: Metadata columns are essential for debugging and auditing.
        # When something goes wrong with your data, these columns tell you:
        #   - WHEN was this data ingested? (_ingested_at)
        #   - WHERE did it come from? (_source_name)
        #   - WHICH batch does it belong to? (_batch_id)
        #   - HAS the source data changed? (_source_hash)
        # At WellMed, every table in your Snowflake warehouse should have
        # similar audit columns.
        """
        if batch_id is None:
            batch_id = str(uuid.uuid4())

        df = df.copy()
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_name"] = self.source_name
        df["_batch_id"] = batch_id

        # Content hash for idempotency checks
        # LEARN: By hashing the row content, we can detect duplicates
        # across multiple ingestion runs. If the hash already exists
        # in Bronze, we know the data hasn't changed.
        df["_source_hash"] = df.apply(
            lambda row: hashlib.md5(
                str(row.to_dict()).encode()
            ).hexdigest(),
            axis=1,
        )

        return df

    def store_bronze(
        self,
        df: pd.DataFrame,
        dataset_name: str,
        batch_id: str | None = None,
    ) -> Path:
        """
        Store raw data as a Parquet file in the Bronze layer.

        File naming convention: {source}/{dataset}_{timestamp}_{batch_id}.parquet

        # LEARN: We use timestamp + batch_id in the filename for two reasons:
        #   1. Immutability — we never overwrite old files, just add new ones
        #   2. Traceability — you can trace any record back to its exact
        #      ingestion batch by matching the batch_id
        # This is the same pattern used in data lakes on S3/ADLS.
        """
        if batch_id is None:
            batch_id = str(uuid.uuid4())[:8]

        # Create source-specific directory
        source_dir = self.bronze_path / self.source_name
        source_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp for immutability
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset_name}_{timestamp}_{batch_id}.parquet"
        file_path = source_dir / filename

        # Write as Parquet using PyArrow (handles complex types better)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")

        logger.info(
            f"Stored {len(df)} rows to Bronze: {file_path}",
            extra={"source": self.source_name, "layer": "bronze"},
        )
        return file_path

    def ingest(
        self,
        endpoint_name: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Full ingestion flow: extract -> add metadata -> store.

        This is the main method called by the orchestrator.

        Returns:
            Dictionary with ingestion results:
            {
                "source": str,
                "endpoint": str,
                "batch_id": str,
                "rows_ingested": int,
                "file_path": str,
                "status": "success" | "error",
                "error": str | None
            }
        """
        batch_id = str(uuid.uuid4())
        dataset_name = endpoint_name or "default"

        result: dict[str, Any] = {
            "source": self.source_name,
            "endpoint": dataset_name,
            "batch_id": batch_id,
            "rows_ingested": 0,
            "file_path": None,
            "status": "error",
            "error": None,
        }

        try:
            logger.info(
                f"Starting ingestion: {self.source_name}/{dataset_name}",
                extra={"source": self.source_name, "layer": "bronze"},
            )

            # Step 1: Extract raw data
            raw_df = self.extract(endpoint_name=endpoint_name, **kwargs)

            if raw_df.empty:
                logger.warning(
                    f"No data returned from {self.source_name}/{dataset_name}",
                    extra={"source": self.source_name, "layer": "bronze"},
                )
                result["status"] = "success"
                return result

            # Step 2: Add metadata columns
            enriched_df = self.add_metadata(raw_df, batch_id=batch_id)

            # Step 3: Store in Bronze layer
            file_path = self.store_bronze(enriched_df, dataset_name, batch_id[:8])

            result["rows_ingested"] = len(enriched_df)
            result["file_path"] = str(file_path)
            result["status"] = "success"

            logger.info(
                f"Ingestion complete: {result['rows_ingested']} rows "
                f"from {self.source_name}/{dataset_name}",
                extra={"source": self.source_name, "layer": "bronze"},
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error(
                f"Ingestion failed for {self.source_name}/{dataset_name}: {e}",
                extra={"source": self.source_name, "layer": "bronze"},
                exc_info=True,
            )

        return result
