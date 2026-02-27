"""
REST API Ingestor
==================

WHAT: Pulls data from REST APIs with support for pagination, rate limiting,
retry logic, and authentication. Currently configured for:
  - Wger Fitness API (exercises, muscles, equipment)
  - USDA FoodData Central (nutrition data)

WHY: Most real-world data sources expose REST APIs. This ingestor handles
the common challenges:
  - Pagination (APIs return data in pages, not all at once)
  - Rate limiting (APIs restrict how fast you can call them)
  - Retry with backoff (network errors happen, retry intelligently)
  - Authentication (API keys, bearer tokens, etc.)

HOW: Uses httpx (modern async-capable HTTP client) with automatic
pagination traversal and exponential backoff on failures.

# LEARN: At WellMed, when you pull data from vendor APIs (EHR systems,
# lab systems, etc.), you'll face these exact same challenges. The
# patterns here — pagination, rate limiting, retry — are universal
# across all API integrations in data engineering.

What Would Break If you ignored rate limiting:
  - The API would return HTTP 429 (Too Many Requests)
  - Your IP could get temporarily or permanently banned
  - The API provider might revoke your credentials
  - In production, this could cascade and break other pipelines
"""

import time
from typing import Any

import httpx
import pandas as pd

from src.ingestion.base_ingestor import BaseIngestor
from src.utils.config_loader import EndpointConfig, SourceConfig, get_api_key
from src.utils.logger import get_logger

logger = get_logger("fittrack.ingestion.api")


class APIIngestor(BaseIngestor):
    """
    Ingestor for REST API data sources.

    Handles pagination, rate limiting, authentication, and retry logic.

    Usage:
        config = load_source_configs()["wger_exercises"]
        ingestor = APIIngestor("wger_exercises", config)
        result = ingestor.ingest(endpoint_name="exercises")
    """

    def __init__(
        self,
        source_name: str,
        source_config: SourceConfig,
        bronze_path: str = "data/bronze",
        max_retries: int = 3,
        base_delay: float = 5.0,
        backoff_factor: float = 2.0,
    ):
        super().__init__(source_name, source_config, bronze_path)
        self.base_url = source_config.base_url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor

        # Load API key if required
        self.api_key: str | None = None
        if source_config.auth.type == "api_key" and source_config.auth.env_var:
            self.api_key = get_api_key(source_config.auth.env_var)

        # Rate limiting state
        self._rate_limit = source_config.rate_limit.requests_per_minute
        self._request_interval = 60.0 / self._rate_limit if self._rate_limit > 0 else 0
        self._last_request_time: float = 0

    def _wait_for_rate_limit(self) -> None:
        """
        Enforce rate limiting between requests.

        # LEARN: Rate limiting prevents you from overwhelming the API server.
        # Professional APIs specify their limits (e.g., "60 requests per minute").
        # We calculate the minimum interval between requests and sleep if needed.
        # This is a "token bucket" approach — simple and effective.
        """
        if self._request_interval <= 0:
            return

        elapsed = time.time() - self._last_request_time
        if elapsed < self._request_interval:
            sleep_time = self._request_interval - elapsed
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _make_request(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Make an HTTP GET request with retry logic and exponential backoff.

        # LEARN: Exponential backoff means each retry waits longer:
        #   Attempt 1: wait 5 seconds
        #   Attempt 2: wait 10 seconds (5 * 2)
        #   Attempt 3: wait 20 seconds (5 * 2 * 2)
        # This prevents hammering a server that's already struggling.
        # AWS, Azure, and every cloud provider recommend this pattern.
        """
        if params is None:
            params = {}

        # Add API key to params if auth requires it
        if self.api_key and self.config.auth.key_param:
            params[self.config.auth.key_param] = self.api_key

        last_exception: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            self._wait_for_rate_limit()

            try:
                with httpx.Client(timeout=30.0) as client:
                    response = client.get(url, params=params)

                    # Handle rate limiting response
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logger.warning(
                            f"Rate limited (429). Waiting {retry_after}s "
                            f"(attempt {attempt}/{self.max_retries})",
                            extra={"source": self.source_name},
                        )
                        time.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    return response.json()

            except httpx.HTTPStatusError as e:
                last_exception = e
                if e.response.status_code >= 500:
                    # Server error — retry with backoff
                    delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
                    logger.warning(
                        f"Server error {e.response.status_code}. "
                        f"Retrying in {delay:.1f}s (attempt {attempt}/{self.max_retries})",
                        extra={"source": self.source_name},
                    )
                    time.sleep(delay)
                else:
                    # Client error (4xx except 429) — don't retry
                    raise

            except (httpx.ConnectError, httpx.ReadTimeout) as e:
                last_exception = e
                delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
                logger.warning(
                    f"Connection error: {e}. "
                    f"Retrying in {delay:.1f}s (attempt {attempt}/{self.max_retries})",
                    extra={"source": self.source_name},
                )
                time.sleep(delay)

        raise ConnectionError(
            f"Failed after {self.max_retries} attempts: {last_exception}"
        )

    def _paginate(
        self,
        endpoint: EndpointConfig,
    ) -> list[dict[str, Any]]:
        """
        Handle API pagination to retrieve all pages of data.

        # LEARN: Most APIs don't return all data in one response.
        # They use pagination — returning 20-100 records per "page".
        # There are three common pagination styles:
        #
        #   1. Offset-based: ?offset=0&limit=100, then offset=100, offset=200...
        #      (Used by Wger API)
        #   2. Cursor-based: Response includes a "next" URL or cursor token
        #      (Used by Slack, Twitter/X APIs)
        #   3. Page-based: ?page=1, ?page=2, ?page=3...
        #      (Used by USDA API)
        #
        # This method handles all three styles based on the config.
        """
        url = f"{self.base_url}{endpoint.path}"
        params = dict(endpoint.params)
        all_records: list[dict[str, Any]] = []
        pagination = endpoint.pagination
        page_count = 0

        if pagination.type == "offset":
            offset = 0
            limit = params.get(pagination.limit_param, 100)

            while True:
                params[pagination.offset_param] = offset
                params[pagination.limit_param] = limit

                logger.debug(
                    f"Fetching page: offset={offset}, limit={limit}",
                    extra={"source": self.source_name},
                )

                data = self._make_request(url, params)
                results = data.get(pagination.results_key, [])
                all_records.extend(results)
                page_count += 1

                # Check if there are more pages
                next_url = data.get(pagination.next_key or "next")
                if not next_url or len(results) == 0:
                    break

                offset += limit

        elif pagination.type == "page":
            page = 1
            while True:
                params[pagination.page_param] = page

                logger.debug(
                    f"Fetching page {page}",
                    extra={"source": self.source_name},
                )

                data = self._make_request(url, params)
                results = data.get(pagination.results_key, [])
                all_records.extend(results)
                page_count += 1

                # Check if we've gotten all records
                total = data.get(pagination.total_key or "totalHits", 0)
                if len(all_records) >= total or len(results) == 0:
                    break

                page += 1

        elif pagination.type == "cursor":
            cursor = None
            while True:
                if cursor:
                    params["cursor"] = cursor

                data = self._make_request(url, params)
                results = data.get(pagination.results_key, [])
                all_records.extend(results)
                page_count += 1

                cursor = data.get(pagination.next_key or "next_cursor")
                if not cursor or len(results) == 0:
                    break

        else:
            # No pagination — single request
            data = self._make_request(url, params)
            if isinstance(data, list):
                all_records = data
            elif isinstance(data, dict):
                all_records = data.get(pagination.results_key, [data])
            page_count = 1

        logger.info(
            f"Pagination complete: {len(all_records)} records "
            f"across {page_count} pages",
            extra={"source": self.source_name},
        )
        return all_records

    def extract(self, endpoint_name: str | None = None, **kwargs: Any) -> pd.DataFrame:
        """
        Extract data from a specific API endpoint.

        Args:
            endpoint_name: Which endpoint to call (from sources.yaml config)
            **kwargs: Additional parameters to pass to the API

        Returns:
            Raw data as a Pandas DataFrame
        """
        if endpoint_name is None:
            # Default to first endpoint
            endpoint_name = next(iter(self.config.endpoints))

        if endpoint_name not in self.config.endpoints:
            raise ValueError(
                f"Unknown endpoint '{endpoint_name}' for source '{self.source_name}'. "
                f"Available: {list(self.config.endpoints.keys())}"
            )

        endpoint = self.config.endpoints[endpoint_name]

        logger.info(
            f"Extracting from {self.source_name}/{endpoint_name}: "
            f"{self.base_url}{endpoint.path}",
            extra={"source": self.source_name, "layer": "bronze"},
        )

        # Fetch all pages of data
        records = self._paginate(endpoint)

        if not records:
            logger.warning(
                f"No records returned from {self.source_name}/{endpoint_name}",
                extra={"source": self.source_name},
            )
            return pd.DataFrame()

        # Normalize nested JSON into flat DataFrame
        # LEARN: API responses are often nested JSON. pd.json_normalize()
        # flattens nested objects into columns with dot notation.
        # e.g., {"user": {"name": "Marcus"}} becomes column "user.name"
        df = pd.json_normalize(records, sep="_")

        logger.info(
            f"Extracted {len(df)} records with {len(df.columns)} columns "
            f"from {endpoint_name}",
            extra={"source": self.source_name, "layer": "bronze"},
        )
        return df
