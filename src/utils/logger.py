"""
Pipeline Logging Utility
========================

WHAT: Provides structured, consistent logging across the entire pipeline.

WHY: In production data pipelines, you NEED structured logs because:
  1. They're machine-parseable (JSON format) for log aggregation tools
     like Splunk, Datadog, or ELK stack
  2. They include context (pipeline_run_id, source, layer) so you can
     trace issues across the Bronze -> Silver -> Gold flow
  3. They rotate automatically so you don't fill up disk space

HOW: Wraps Python's built-in logging module with:
  - JSON formatter for structured logs to files
  - Rich console formatter for human-readable output during development
  - Automatic log rotation (max 50MB per file, 5 backups)
  - Context injection (run_id, source, layer)

# LEARN: Never use print() in production code. Logging gives you:
#   - Severity levels (DEBUG/INFO/WARNING/ERROR/CRITICAL)
#   - Timestamps automatically
#   - File output without changing code
#   - The ability to filter by severity in production
#   At WellMed, your ETL logs are probably the first place anyone looks
#   when something breaks at 2 AM.
"""

import json
import logging
import logging.handlers
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JSONFormatter(logging.Formatter):
    """
    Formats log records as JSON lines.

    # LEARN: JSON logs are the industry standard for production systems.
    # Tools like Splunk, Datadog, CloudWatch, and ELK can parse them
    # automatically. Plain text logs require custom parsing rules.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add any extra context fields (run_id, source, etc.)
        if hasattr(record, "run_id"):
            log_entry["run_id"] = record.run_id
        if hasattr(record, "source"):
            log_entry["source"] = record.source
        if hasattr(record, "layer"):
            log_entry["layer"] = record.layer
        if hasattr(record, "duration_ms"):
            log_entry["duration_ms"] = record.duration_ms

        # Include exception info if present
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else "Unknown",
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, default=str)


class ConsoleFormatter(logging.Formatter):
    """
    Human-readable colored console output for development.
    Uses simple formatting without external dependencies.
    """

    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[41m",  # Red background
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")

        # Build context string from extra fields
        context_parts: list[str] = []
        if hasattr(record, "layer"):
            context_parts.append(f"[{record.layer}]")
        if hasattr(record, "source"):
            context_parts.append(f"({record.source})")
        context = " ".join(context_parts)
        if context:
            context = f" {context}"

        return (
            f"{color}{timestamp} {record.levelname:<8}{self.RESET}"
            f"{context} {record.getMessage()}"
        )


def setup_logger(
    name: str = "fittrack",
    log_dir: str = "logs",
    level: str = "INFO",
    json_logs: bool = True,
    console_output: bool = True,
    max_file_size_mb: int = 50,
    backup_count: int = 5,
) -> logging.Logger:
    """
    Create and configure a logger for the pipeline.

    Args:
        name: Logger name (used for hierarchy, e.g., 'fittrack.ingestion')
        log_dir: Directory for log files
        level: Minimum log level to capture
        json_logs: Write JSON-formatted logs to files
        console_output: Also output to console
        max_file_size_mb: Max size before log rotation
        backup_count: Number of rotated log files to keep

    Returns:
        Configured logging.Logger instance

    # LEARN: Logger hierarchy in Python works with dots:
    #   'fittrack'            -> root pipeline logger
    #   'fittrack.ingestion'  -> inherits settings from 'fittrack'
    #   'fittrack.quality'    -> also inherits from 'fittrack'
    # This means you set up the root logger once, and child loggers
    # automatically get the same handlers and formatters.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if setup is called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # File handler with rotation (JSON format)
    if json_logs:
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_path / f"{name}.log",
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(JSONFormatter())
        logger.addHandler(file_handler)

    # Console handler (human-readable)
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ConsoleFormatter())
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str = "fittrack") -> logging.Logger:
    """
    Get an existing logger by name. If it hasn't been set up,
    creates one with default settings.

    This is the function you'll call in every module:
        from src.utils.logger import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logger(name)
    return logger
