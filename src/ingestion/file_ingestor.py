"""
File-Based Ingestor
====================

WHAT: Ingests data from CSV and JSON files dropped into a "watch directory"
(data/incoming/). Simulates what a real gym app or fitness tracker export
would look like.

WHY: Not all data comes from APIs. Many real-world data sources produce
flat files:
  - Database exports (CSV dumps from legacy systems)
  - Application exports (fitness tracker data, spreadsheet exports)
  - Vendor data drops (partners uploading files to SFTP/S3)
  - Manual data entry (coaches uploading workout logs)

HOW: Scans the incoming directory for matching files, validates their
format and schema, and stores them in the Bronze layer. Processed files
are moved to a "processed" directory; malformed files go to "errors".

# LEARN: At WellMed, you probably receive data files from external
# vendors or partners. This "file drop zone" pattern is exactly how
# that works — files land in a directory (or S3 bucket), your pipeline
# picks them up, processes them, and moves them out. The key principles:
#   1. Never modify the original file
#   2. Move (don't delete) after processing
#   3. Keep error files for debugging
#   4. Validate before processing

What Would Break If you didn't move processed files:
  - The pipeline would re-process the same file every run
  - Duplicate data would accumulate in your warehouse
  - Storage costs would grow unnecessarily
  - Query results would be wrong due to double-counting
"""

import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion.base_ingestor import BaseIngestor
from src.utils.config_loader import SourceConfig
from src.utils.logger import get_logger

logger = get_logger("fittrack.ingestion.file")


class FileIngestor(BaseIngestor):
    """
    Ingestor for file-based data sources (CSV, JSON).

    Watches a directory for new files, validates them, and stores
    the raw data in the Bronze layer.

    Usage:
        config = load_source_configs()["file_drop_zone"]
        ingestor = FileIngestor("file_drop_zone", config)
        result = ingestor.ingest(endpoint_name="workout_logs")
    """

    # Maximum file size to process (in bytes)
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

    def __init__(
        self,
        source_name: str,
        source_config: SourceConfig,
        bronze_path: str = "data/bronze",
    ):
        super().__init__(source_name, source_config, bronze_path)

        self.watch_dir = Path(source_config.watch_directory)
        self.watch_dir.mkdir(parents=True, exist_ok=True)

        # Create processed and error directories
        self.processed_dir = self.watch_dir / "processed"
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        self.error_dir = self.watch_dir / "errors"
        self.error_dir.mkdir(parents=True, exist_ok=True)

        self.supported_formats = source_config.supported_formats or [".csv", ".json"]
        self.file_patterns = source_config.file_patterns or {}

    def _discover_files(self, pattern: str | None = None) -> list[Path]:
        """
        Find files in the watch directory matching the given pattern.

        # LEARN: File discovery is the first step in file-based ingestion.
        # We look for files matching specific patterns because the watch
        # directory might contain files for different datasets
        # (workout logs vs. body metrics vs. nutrition data).
        """
        files: list[Path] = []

        if pattern:
            # Expand pattern like "workout_*.{csv,json}" into actual globs
            for ext in self.supported_formats:
                # Also try with the dot prefix
                for match in self.watch_dir.glob(pattern.split(".")[0].replace("*", "**/*") + ext):
                    if match.is_file():
                        files.append(match)
                # Direct glob
                for match in self.watch_dir.glob(f"*{ext}"):
                    if match.is_file() and match not in files:
                        files.append(match)
        else:
            # Find all supported files
            for ext in self.supported_formats:
                for match in self.watch_dir.glob(f"*{ext}"):
                    if match.is_file():
                        files.append(match)

        # Filter out files that are too large
        valid_files = []
        for f in files:
            if f.stat().st_size > self.MAX_FILE_SIZE:
                logger.warning(
                    f"File too large ({f.stat().st_size / 1024 / 1024:.1f} MB), " f"skipping: {f.name}",
                    extra={"source": self.source_name},
                )
            else:
                valid_files.append(f)

        logger.info(
            f"Discovered {len(valid_files)} files in {self.watch_dir}",
            extra={"source": self.source_name},
        )
        return sorted(valid_files)

    def _read_file(self, file_path: Path) -> pd.DataFrame:
        """
        Read a single file (CSV or JSON) into a DataFrame.

        # LEARN: Error handling for file reading is critical because
        # external files are the least trustworthy data source.
        # They can be:
        #   - Malformed (missing columns, wrong encoding)
        #   - Truncated (upload was interrupted)
        #   - Wrong format (someone uploaded an Excel file as .csv)
        #   - Empty
        """
        suffix = file_path.suffix.lower()

        try:
            if suffix == ".csv":
                # Try UTF-8 first, fall back to Latin-1
                try:
                    df = pd.read_csv(file_path, encoding="utf-8")
                except UnicodeDecodeError:
                    logger.warning(
                        f"UTF-8 decode failed for {file_path.name}, " f"trying Latin-1",
                        extra={"source": self.source_name},
                    )
                    df = pd.read_csv(file_path, encoding="latin-1")

            elif suffix == ".json":
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)

                # Handle both JSON array and JSON object with a data key
                if isinstance(data, list):
                    df = pd.json_normalize(data, sep="_")
                elif isinstance(data, dict):
                    # Look for common data keys
                    for key in ["data", "records", "results", "items"]:
                        if key in data and isinstance(data[key], list):
                            df = pd.json_normalize(data[key], sep="_")
                            break
                    else:
                        # Single record
                        df = pd.json_normalize([data], sep="_")
            else:
                raise ValueError(f"Unsupported file format: {suffix}")

            if df.empty:
                logger.warning(
                    f"File is empty: {file_path.name}",
                    extra={"source": self.source_name},
                )

            logger.info(
                f"Read {len(df)} rows from {file_path.name} " f"({len(df.columns)} columns)",
                extra={"source": self.source_name},
            )
            return df

        except Exception as e:
            logger.error(
                f"Failed to read {file_path.name}: {e}",
                extra={"source": self.source_name},
            )
            # Move to error directory
            self._move_to_errors(file_path, str(e))
            return pd.DataFrame()

    def _move_to_processed(self, file_path: Path) -> None:
        """Move a successfully processed file to the processed directory."""
        dest = self.processed_dir / file_path.name
        # Handle name collision by appending a counter
        counter = 1
        while dest.exists():
            stem = file_path.stem
            dest = self.processed_dir / f"{stem}_{counter}{file_path.suffix}"
            counter += 1

        shutil.move(str(file_path), str(dest))
        logger.debug(
            f"Moved processed file: {file_path.name} -> processed/",
            extra={"source": self.source_name},
        )

    def _move_to_errors(self, file_path: Path, error_msg: str) -> None:
        """Move a failed file to the errors directory with an error log."""
        dest = self.error_dir / file_path.name
        if file_path.exists():
            shutil.move(str(file_path), str(dest))

        # Write error details
        error_log = self.error_dir / f"{file_path.stem}_error.txt"
        error_log.write_text(f"File: {file_path.name}\nError: {error_msg}\n")

        logger.debug(
            f"Moved error file: {file_path.name} -> errors/",
            extra={"source": self.source_name},
        )

    def extract(self, endpoint_name: str | None = None, **kwargs: Any) -> pd.DataFrame:
        """
        Extract data from files in the watch directory.

        Args:
            endpoint_name: File pattern key from config (e.g., 'workout_logs').
                           If None, processes all files.

        Returns:
            Combined DataFrame from all matching files.
        """
        # Determine which file pattern to use
        pattern = None
        if endpoint_name and endpoint_name in self.file_patterns:
            pattern = self.file_patterns[endpoint_name]

        # Discover matching files
        files = self._discover_files(pattern)

        if not files:
            logger.info(
                f"No files to process in {self.watch_dir}",
                extra={"source": self.source_name, "layer": "bronze"},
            )
            return pd.DataFrame()

        # Read and combine all files
        all_dfs: list[pd.DataFrame] = []
        for file_path in files:
            df = self._read_file(file_path)
            if not df.empty:
                # Add file source metadata
                df["_source_file"] = file_path.name
                all_dfs.append(df)
                # Move processed file
                self._move_to_processed(file_path)

        if not all_dfs:
            return pd.DataFrame()

        # Combine all DataFrames
        combined = pd.concat(all_dfs, ignore_index=True)

        logger.info(
            f"Extracted {len(combined)} total rows from {len(all_dfs)} files",
            extra={"source": self.source_name, "layer": "bronze"},
        )
        return combined
