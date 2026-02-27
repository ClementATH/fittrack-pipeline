"""
Configuration Loader
====================

WHAT: Loads, validates, and provides access to all YAML configuration files.

WHY: A centralized config loader means:
  1. Every module gets config the same way (consistency)
  2. Config is validated at startup (fail fast, not mid-pipeline)
  3. You can override settings with environment variables (12-factor app)

HOW: Uses Pydantic for type-safe config models, YAML for file format,
and environment variables for secrets (API keys, passwords).

# LEARN: The "12-Factor App" methodology says config should be stored
# in the environment, not in code. That's why API keys come from
# environment variables, while non-secret settings live in YAML files.
# This pattern is standard at every company you'll work at.
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from src.utils.logger import get_logger

logger = get_logger("fittrack.config")


# ============================================================
# Pydantic Models for Type-Safe Configuration
# ============================================================
# LEARN: Pydantic models validate your config at load time.
# If someone puts a string where a number should be, you get
# a clear error message immediately — not a cryptic crash
# 500 rows into processing.
# ============================================================


class DatabaseConfig(BaseModel):
    path: str = "data/fittrack.duckdb"
    read_only: bool = False
    threads: int = 4


class RetryConfig(BaseModel):
    max_attempts: int = 3
    base_delay_seconds: int = 5
    backoff_factor: int = 2
    max_delay_seconds: int = 60


class LoggingConfig(BaseModel):
    level: str = "INFO"
    log_dir: str = "logs"
    json_logs: bool = True
    console_output: bool = True
    max_file_size_mb: int = 50
    backup_count: int = 5


class BronzeConfig(BaseModel):
    storage_path: str = "data/bronze"
    storage_format: str = "parquet"
    add_metadata: bool = True


class SilverConfig(BaseModel):
    storage_path: str = "data/silver"
    storage_format: str = "parquet"
    naming_convention: str = "snake_case"


class GoldConfig(BaseModel):
    storage_path: str = "data/gold"
    storage_format: str = "parquet"


class SchedulingConfig(BaseModel):
    enabled: bool = False
    full_pipeline_cron: str = "0 6 * * *"
    quality_check_cron: str = "0 */4 * * *"
    health_check_cron: str = "*/15 * * * *"


class MonitoringConfig(BaseModel):
    health_check_interval_seconds: int = 900
    alert_log_path: str = "logs/alerts.json"


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration."""
    name: str = "fittrack-etl-pipeline"
    version: str = "1.0.0"
    environment: str = "development"
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    bronze: BronzeConfig = Field(default_factory=BronzeConfig)
    silver: SilverConfig = Field(default_factory=SilverConfig)
    gold: GoldConfig = Field(default_factory=GoldConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    scheduling: SchedulingConfig = Field(default_factory=SchedulingConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


class RateLimitConfig(BaseModel):
    requests_per_minute: int = 60
    retry_on_429: bool = True


class PaginationConfig(BaseModel):
    type: str = "offset"
    limit_param: str = "limit"
    offset_param: str = "offset"
    results_key: str = "results"
    next_key: str | None = "next"
    page_param: str = "pageNumber"
    total_key: str | None = None


class EndpointConfig(BaseModel):
    path: str
    params: dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)


class AuthConfig(BaseModel):
    type: str = "none"
    key_param: str | None = None
    env_var: str | None = None


class SourceScheduleConfig(BaseModel):
    frequency: str = "daily"
    incremental: bool = False
    poll_interval_seconds: int = 30


class SourceConfig(BaseModel):
    """Configuration for a single data source."""
    type: str
    description: str = ""
    base_url: str = ""
    endpoints: dict[str, EndpointConfig] = Field(default_factory=dict)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    schedule: SourceScheduleConfig = Field(default_factory=SourceScheduleConfig)
    watch_directory: str = ""
    supported_formats: list[str] = Field(default_factory=list)
    file_patterns: dict[str, str] = Field(default_factory=dict)


# ============================================================
# Config Loader Functions
# ============================================================


def _find_project_root() -> Path:
    """
    Find the project root by looking for pyproject.toml.

    # LEARN: This pattern ensures the pipeline works regardless of
    # where it's invoked from (project root, src/, tests/, etc.).
    """
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current


def load_yaml(file_path: Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dictionary."""
    if not file_path.exists():
        logger.warning(f"Config file not found: {file_path}")
        return {}

    with open(file_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
        return data if data is not None else {}


def load_pipeline_config(config_path: Path | None = None) -> PipelineConfig:
    """
    Load the main pipeline configuration.

    Args:
        config_path: Path to pipeline_config.yaml. If None, auto-discovers.

    Returns:
        Validated PipelineConfig instance.
    """
    if config_path is None:
        root = _find_project_root()
        config_path = root / "config" / "pipeline_config.yaml"

    raw = load_yaml(config_path)

    # Flatten nested 'pipeline' key if present
    pipeline_data = raw.get("pipeline", {})
    merged = {**raw, **pipeline_data}
    # Remove the nested key to avoid Pydantic confusion
    merged.pop("pipeline", None)

    config = PipelineConfig(**merged)
    logger.info(
        f"Pipeline config loaded: {config.name} v{config.version} "
        f"({config.environment})"
    )
    return config


def load_source_configs(config_path: Path | None = None) -> dict[str, SourceConfig]:
    """
    Load data source configurations.

    Returns:
        Dictionary mapping source name -> SourceConfig
    """
    if config_path is None:
        root = _find_project_root()
        config_path = root / "config" / "sources.yaml"

    raw = load_yaml(config_path)
    sources_raw = raw.get("sources", {})

    sources: dict[str, SourceConfig] = {}
    for name, source_data in sources_raw.items():
        # Convert nested endpoint dicts to EndpointConfig
        endpoints = {}
        for ep_name, ep_data in source_data.get("endpoints", {}).items():
            endpoints[ep_name] = EndpointConfig(**ep_data)
        source_data["endpoints"] = endpoints

        sources[name] = SourceConfig(**source_data)
        logger.debug(f"Source config loaded: {name} ({sources[name].type})")

    logger.info(f"Loaded {len(sources)} source configurations")
    return sources


def load_quality_rules(config_path: Path | None = None) -> dict[str, Any]:
    """
    Load data quality rules configuration.

    Returns:
        Dictionary with schema_rules, business_rules, freshness_rules,
        and anomaly_detection settings.
    """
    if config_path is None:
        root = _find_project_root()
        config_path = root / "config" / "quality_rules.yaml"

    rules = load_yaml(config_path)
    logger.info(
        f"Quality rules loaded: "
        f"{len(rules.get('schema_rules', {}))} schema rules, "
        f"{len(rules.get('business_rules', {}))} business rule sets, "
        f"{len(rules.get('freshness_rules', {}))} freshness rules"
    )
    return rules


def get_api_key(env_var: str) -> str | None:
    """
    Safely retrieve an API key from environment variables.

    # LEARN: NEVER hardcode API keys in source code. They go in:
    #   1. Environment variables (best for production)
    #   2. .env files (for local development, added to .gitignore)
    #   3. Secret managers (AWS Secrets Manager, Azure Key Vault)
    # At WellMed, your Snowflake credentials should follow this pattern.
    """
    value = os.environ.get(env_var)
    if value is None:
        logger.warning(
            f"Environment variable '{env_var}' not set. "
            f"Some features may not work."
        )
    return value
