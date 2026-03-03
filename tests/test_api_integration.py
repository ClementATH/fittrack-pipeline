"""
API Integration Tests
======================

Integration tests for APIIngestor with mocked HTTP responses. All tests
are offline — no live API calls. Verifies pagination, rate limiting, retry,
auth, and the full Bronze ingestion flow.

# LEARN: Mocked integration tests let you verify API logic without
# depending on external services. In CI, tests must be deterministic —
# a flaky test caused by a slow API wastes everyone's time.
"""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
import pytest

from src.ingestion.api_ingestor import APIIngestor
from src.utils.config_loader import (
    AuthConfig,
    EndpointConfig,
    PaginationConfig,
    RateLimitConfig,
    SourceConfig,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "api_responses"


# ============================================================
# Helpers
# ============================================================
def load_fixture(name: str) -> dict:
    """Load a JSON fixture file."""
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _make_mock_response(data: dict, status_code: int = 200) -> MagicMock:
    """Create a mock httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.json.return_value = data
    response.status_code = status_code
    response.headers = {}
    if status_code < 400:
        response.raise_for_status = MagicMock()
    else:
        http_error = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=response,
        )
        response.raise_for_status.side_effect = http_error
    return response


def _build_wger_config() -> SourceConfig:
    """Build a minimal Wger source config for testing."""
    return SourceConfig(
        type="rest_api",
        description="Wger test",
        base_url="https://wger.de/api/v2",
        endpoints={
            "exercises": EndpointConfig(
                path="/exercise/",
                params={"format": "json", "limit": 3},
                pagination=PaginationConfig(
                    type="offset",
                    limit_param="limit",
                    offset_param="offset",
                    results_key="results",
                    next_key="next",
                ),
            ),
            "muscles": EndpointConfig(
                path="/muscle/",
                params={"format": "json"},
                pagination=PaginationConfig(
                    type="offset",
                    results_key="results",
                    next_key="next",
                ),
            ),
        },
        rate_limit=RateLimitConfig(requests_per_minute=600),
        auth=AuthConfig(type="none"),
    )


def _build_usda_config() -> SourceConfig:
    """Build a minimal USDA source config for testing."""
    return SourceConfig(
        type="rest_api",
        description="USDA test",
        base_url="https://api.nal.usda.gov/fdc/v1",
        endpoints={
            "foods_search": EndpointConfig(
                path="/foods/search",
                params={"query": "chicken", "pageSize": 2},
                pagination=PaginationConfig(
                    type="page",
                    page_param="pageNumber",
                    results_key="foods",
                    total_key="totalHits",
                ),
            ),
        },
        rate_limit=RateLimitConfig(requests_per_minute=600),
        auth=AuthConfig(type="api_key", key_param="api_key", env_var="USDA_API_KEY"),
    )


# ============================================================
# Wger API tests
# ============================================================
class TestWgerAPIIntegration:
    """Test Wger API ingestion with mocked responses."""

    def test_extract_exercises_with_pagination(self, tmp_path: Path) -> None:
        """Full extraction should paginate through all pages."""
        config = _build_wger_config()
        ingestor = APIIngestor("wger_exercises", config, bronze_path=str(tmp_path))

        page1 = load_fixture("wger_exercises_page1.json")
        page2 = load_fixture("wger_exercises_page2.json")

        mock_client = MagicMock()
        mock_client.get.side_effect = [
            _make_mock_response(page1),
            _make_mock_response(page2),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("exercises")

        assert len(df) == 5
        assert "name" in df.columns
        assert set(df["name"]) == {
            "Barbell Bench Press",
            "Barbell Deadlift",
            "Barbell Squat",
            "Overhead Press",
            "Barbell Row",
        }

    def test_full_bronze_ingest_flow(self, tmp_path: Path) -> None:
        """ingest() should extract, add metadata, and store as Parquet."""
        config = _build_wger_config()
        ingestor = APIIngestor("wger_exercises", config, bronze_path=str(tmp_path))

        page1 = load_fixture("wger_exercises_page1.json")
        page2 = load_fixture("wger_exercises_page2.json")

        mock_client = MagicMock()
        mock_client.get.side_effect = [
            _make_mock_response(page1),
            _make_mock_response(page2),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            result = ingestor.ingest("exercises")

        assert result["status"] == "success"
        assert result["rows_ingested"] == 5
        assert Path(result["file_path"]).exists()

        # Verify Parquet is readable
        stored_df = pd.read_parquet(result["file_path"])
        assert len(stored_df) == 5

    def test_extract_muscles_single_page(self, tmp_path: Path) -> None:
        """Single-page endpoint should work without extra pagination."""
        config = _build_wger_config()
        ingestor = APIIngestor("wger_exercises", config, bronze_path=str(tmp_path))

        muscles_data = load_fixture("wger_muscles.json")

        mock_client = MagicMock()
        mock_client.get.return_value = _make_mock_response(muscles_data)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("muscles")

        assert len(df) == 4
        assert "name" in df.columns

    def test_metadata_columns_present(self, tmp_path: Path) -> None:
        """Bronze output should contain metadata columns."""
        config = _build_wger_config()
        ingestor = APIIngestor("wger_exercises", config, bronze_path=str(tmp_path))

        muscles_data = load_fixture("wger_muscles.json")

        mock_client = MagicMock()
        mock_client.get.return_value = _make_mock_response(muscles_data)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            result = ingestor.ingest("muscles")

        stored_df = pd.read_parquet(result["file_path"])
        assert "_ingested_at" in stored_df.columns
        assert "_source_name" in stored_df.columns
        assert "_batch_id" in stored_df.columns
        assert "_source_hash" in stored_df.columns


# ============================================================
# USDA API tests
# ============================================================
class TestUSDAAPIIntegration:
    """Test USDA API ingestion with mocked responses."""

    def test_extract_foods_search_with_pagination(self, tmp_path: Path) -> None:
        """USDA page-based pagination should traverse all pages."""
        config = _build_usda_config()
        ingestor = APIIngestor("usda_nutrition", config, bronze_path=str(tmp_path))

        page1 = load_fixture("usda_foods_search_page1.json")
        page2 = load_fixture("usda_foods_search_page2.json")

        mock_client = MagicMock()
        mock_client.get.side_effect = [
            _make_mock_response(page1),
            _make_mock_response(page2),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("foods_search")

        assert len(df) == 4
        assert "description" in df.columns

    def test_api_key_injection(self, tmp_path: Path) -> None:
        """USDA requests should include the API key parameter."""
        config = _build_usda_config()

        with patch.dict("os.environ", {"USDA_API_KEY": "test_key_123"}):
            ingestor = APIIngestor("usda_nutrition", config, bronze_path=str(tmp_path))

        assert ingestor.api_key == "test_key_123"


# ============================================================
# Resilience tests
# ============================================================
class TestAPIResilience:
    """Test retry, rate limiting, and error handling."""

    def test_retry_on_5xx(self, tmp_path: Path) -> None:
        """5xx responses should trigger retry with backoff."""
        config = _build_wger_config()
        ingestor = APIIngestor(
            "wger_exercises",
            config,
            bronze_path=str(tmp_path),
            max_retries=3,
            base_delay=0.01,
            backoff_factor=1.0,
        )

        muscles_data = load_fixture("wger_muscles.json")

        # First call: 500 error, second call: success
        mock_client = MagicMock()
        mock_client.get.side_effect = [
            _make_mock_response({}, status_code=500),
            _make_mock_response(muscles_data),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("muscles")

        assert len(df) == 4
        assert mock_client.get.call_count == 2

    def test_rate_limiting_delay(self, tmp_path: Path) -> None:
        """Requests should respect rate limiting intervals."""
        config = _build_wger_config()
        config.rate_limit.requests_per_minute = 6000  # 100/sec = 0.01s interval
        ingestor = APIIngestor("wger_exercises", config, bronze_path=str(tmp_path))

        muscles_data = load_fixture("wger_muscles.json")

        mock_client = MagicMock()
        mock_client.get.return_value = _make_mock_response(muscles_data)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            start = time.time()
            ingestor.extract("muscles")
            elapsed = time.time() - start

        # Should complete quickly with high rate limit
        assert elapsed < 5.0

    def test_429_rate_limit_response(self, tmp_path: Path) -> None:
        """HTTP 429 should cause wait and retry."""
        config = _build_wger_config()
        ingestor = APIIngestor(
            "wger_exercises",
            config,
            bronze_path=str(tmp_path),
            max_retries=3,
            base_delay=0.01,
        )

        muscles_data = load_fixture("wger_muscles.json")

        # 429 response then success
        resp_429 = MagicMock(spec=httpx.Response)
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "0"}
        resp_429.json.return_value = {}
        resp_429.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.side_effect = [
            resp_429,
            _make_mock_response(muscles_data),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("muscles")

        assert len(df) == 4

    def test_4xx_client_error_no_retry(self, tmp_path: Path) -> None:
        """4xx errors (except 429) should raise immediately."""
        config = _build_wger_config()
        ingestor = APIIngestor(
            "wger_exercises",
            config,
            bronze_path=str(tmp_path),
            max_retries=3,
            base_delay=0.01,
        )

        mock_client = MagicMock()
        mock_client.get.return_value = _make_mock_response({}, status_code=404)
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client), pytest.raises(httpx.HTTPStatusError):
            ingestor.extract("muscles")

        # Should only have been called once (no retry)
        assert mock_client.get.call_count == 1

    def test_connection_error_retry(self, tmp_path: Path) -> None:
        """Connection errors should trigger retry with backoff."""
        config = _build_wger_config()
        ingestor = APIIngestor(
            "wger_exercises",
            config,
            bronze_path=str(tmp_path),
            max_retries=2,
            base_delay=0.01,
            backoff_factor=1.0,
        )

        muscles_data = load_fixture("wger_muscles.json")

        mock_client = MagicMock()
        mock_client.get.side_effect = [
            httpx.ConnectError("Connection refused"),
            _make_mock_response(muscles_data),
        ]
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("httpx.Client", return_value=mock_client):
            df = ingestor.extract("muscles")

        assert len(df) == 4
        assert mock_client.get.call_count == 2
