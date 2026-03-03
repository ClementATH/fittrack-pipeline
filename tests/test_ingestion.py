"""
Tests: Bronze Layer (Ingestion)
================================

Tests for the ingestion module: BaseIngestor metadata enrichment,
FileIngestor file reading and processing, and Parquet storage.

# LEARN: Good tests follow the AAA pattern:
#   Arrange: Set up test data and conditions
#   Act: Call the method being tested
#   Assert: Verify the results
# Each test should test ONE thing and have a clear name that
# explains what it's testing and what the expected outcome is.
"""

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.ingestion.file_ingestor import FileIngestor
from src.utils.config_loader import SourceConfig

# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def file_source_config(tmp_data_dir: Path) -> SourceConfig:
    """Create a SourceConfig for the file ingestor."""
    incoming = tmp_data_dir / "incoming"
    return SourceConfig(
        type="file",
        description="Test file drop zone",
        watch_directory=str(incoming),
        supported_formats=[".csv", ".json"],
        file_patterns={"workout_logs": "workout_*.csv"},
    )


@pytest.fixture
def file_ingestor(file_source_config: SourceConfig, tmp_data_dir: Path) -> FileIngestor:
    """Create a FileIngestor instance for testing."""
    return FileIngestor(
        source_name="test_file_zone",
        source_config=file_source_config,
        bronze_path=str(tmp_data_dir / "bronze"),
    )


# ============================================================
# BaseIngestor Tests (via FileIngestor since Base is abstract)
# ============================================================


class TestBaseIngestorMetadata:
    """Test metadata enrichment provided by BaseIngestor."""

    def test_add_metadata_adds_required_columns(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """Metadata enrichment adds _ingested_at, _source_name, _batch_id, _source_hash."""
        result = file_ingestor.add_metadata(sample_exercises_df)

        assert "_ingested_at" in result.columns
        assert "_source_name" in result.columns
        assert "_batch_id" in result.columns
        assert "_source_hash" in result.columns

    def test_add_metadata_preserves_original_columns(
        self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame
    ):
        """Metadata enrichment should not drop any original columns."""
        original_cols = set(sample_exercises_df.columns)
        result = file_ingestor.add_metadata(sample_exercises_df)
        assert original_cols.issubset(set(result.columns))

    def test_add_metadata_source_name_matches(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """The _source_name should match the ingestor's source_name."""
        result = file_ingestor.add_metadata(sample_exercises_df)
        assert (result["_source_name"] == "test_file_zone").all()

    def test_add_metadata_batch_id_consistent(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """All rows in a batch should share the same batch_id."""
        result = file_ingestor.add_metadata(sample_exercises_df, batch_id="test-batch-123")
        assert (result["_batch_id"] == "test-batch-123").all()

    def test_add_metadata_hash_uniqueness(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """Each unique row should get a unique hash."""
        result = file_ingestor.add_metadata(sample_exercises_df)
        assert result["_source_hash"].nunique() == len(result)

    def test_add_metadata_does_not_modify_original(
        self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame
    ):
        """add_metadata should not mutate the input DataFrame."""
        original_len = len(sample_exercises_df.columns)
        file_ingestor.add_metadata(sample_exercises_df)
        assert len(sample_exercises_df.columns) == original_len


# ============================================================
# Bronze Storage Tests
# ============================================================


class TestBronzeStorage:
    """Test Parquet storage in the Bronze layer."""

    def test_store_bronze_creates_parquet_file(
        self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame, tmp_data_dir: Path
    ):
        """store_bronze should create a valid Parquet file."""
        path = file_ingestor.store_bronze(sample_exercises_df, "exercises", "test-batch")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_store_bronze_parquet_readable(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """The stored Parquet file should be readable by PyArrow."""
        path = file_ingestor.store_bronze(sample_exercises_df, "exercises", "test-batch")
        table = pq.read_table(path)
        assert table.num_rows == len(sample_exercises_df)

    def test_store_bronze_preserves_data(self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame):
        """Data written to Parquet should be identical when read back."""
        path = file_ingestor.store_bronze(sample_exercises_df, "exercises")
        df_read = pd.read_parquet(path)
        assert len(df_read) == len(sample_exercises_df)
        assert set(df_read.columns) == set(sample_exercises_df.columns)

    def test_store_bronze_source_subdirectory(
        self, file_ingestor: FileIngestor, sample_exercises_df: pd.DataFrame, tmp_data_dir: Path
    ):
        """Files should be stored in a source-specific subdirectory."""
        path = file_ingestor.store_bronze(sample_exercises_df, "exercises")
        assert "test_file_zone" in str(path)


# ============================================================
# FileIngestor Tests
# ============================================================


class TestFileIngestor:
    """Test the FileIngestor's file reading and processing."""

    def test_read_csv_file(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """FileIngestor should read CSV files correctly."""
        csv_content = "name,value\nfoo,1\nbar,2\n"
        csv_path = tmp_data_dir / "incoming" / "test.csv"
        csv_path.write_text(csv_content)

        df = file_ingestor._read_file(csv_path)
        assert len(df) == 2
        assert "name" in df.columns
        assert "value" in df.columns

    def test_read_json_array_file(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """FileIngestor should read JSON array files correctly."""
        data = [{"name": "Squat", "reps": 5}, {"name": "Bench", "reps": 8}]
        json_path = tmp_data_dir / "incoming" / "test.json"
        json_path.write_text(json.dumps(data))

        df = file_ingestor._read_file(json_path)
        assert len(df) == 2
        assert "name" in df.columns

    def test_read_json_object_with_data_key(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """FileIngestor should handle JSON objects with a 'data' key."""
        data = {"data": [{"name": "Pull-up", "reps": 10}]}
        json_path = tmp_data_dir / "incoming" / "test_obj.json"
        json_path.write_text(json.dumps(data))

        df = file_ingestor._read_file(json_path)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Pull-up"

    def test_discover_files_finds_supported_formats(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """File discovery should find CSV and JSON files."""
        (tmp_data_dir / "incoming" / "a.csv").write_text("col\n1")
        (tmp_data_dir / "incoming" / "b.json").write_text("[]")
        (tmp_data_dir / "incoming" / "c.txt").write_text("skip")

        files = file_ingestor._discover_files()
        extensions = {f.suffix for f in files}
        assert ".csv" in extensions
        assert ".json" in extensions
        assert ".txt" not in extensions

    def test_move_to_processed(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """After processing, files should be moved to processed/."""
        csv_path = tmp_data_dir / "incoming" / "done.csv"
        csv_path.write_text("col\n1")

        file_ingestor._move_to_processed(csv_path)

        assert not csv_path.exists()
        assert (tmp_data_dir / "incoming" / "processed" / "done.csv").exists()

    def test_move_to_errors(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """Bad files should be moved to errors/ with an error log."""
        bad_path = tmp_data_dir / "incoming" / "bad.csv"
        bad_path.write_text("corrupt")

        file_ingestor._move_to_errors(bad_path, "Parse error")

        assert (tmp_data_dir / "incoming" / "errors" / "bad.csv").exists()
        error_log = tmp_data_dir / "incoming" / "errors" / "bad_error.txt"
        assert error_log.exists()
        assert "Parse error" in error_log.read_text()

    def test_full_ingest_flow(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """Full ingest() should extract, add metadata, and store in Bronze."""
        csv_content = "name,reps\nSquat,5\nBench,8\n"
        (tmp_data_dir / "incoming" / "workout.csv").write_text(csv_content)

        result = file_ingestor.ingest()

        assert result["status"] == "success"
        assert result["rows_ingested"] == 2
        assert result["file_path"] is not None
        # File should have been moved to processed
        assert not (tmp_data_dir / "incoming" / "workout.csv").exists()

    def test_ingest_empty_directory(self, file_ingestor: FileIngestor):
        """Ingesting with no files should return success with 0 rows."""
        result = file_ingestor.ingest()
        assert result["status"] == "success"
        assert result["rows_ingested"] == 0

    def test_file_size_limit(self, file_ingestor: FileIngestor, tmp_data_dir: Path):
        """Files exceeding MAX_FILE_SIZE should be skipped."""
        # Save the original limit and set to 100 bytes for testing
        original = FileIngestor.MAX_FILE_SIZE
        FileIngestor.MAX_FILE_SIZE = 100

        large_content = "name,value\n" + ("x" * 50 + ",1\n") * 10
        (tmp_data_dir / "incoming" / "large.csv").write_text(large_content)

        files = file_ingestor._discover_files()
        # The large file should be filtered out
        assert all(f.stat().st_size <= 100 for f in files)

        FileIngestor.MAX_FILE_SIZE = original
