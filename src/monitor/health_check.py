"""
Health Check Module
====================

WHAT: Verifies system health — source connectivity, storage availability,
database accessibility, and memory usage.

WHY: Health checks catch infrastructure issues before they cause pipeline
failures. Running these proactively means you fix problems before data
gets stale or pipelines start erroring.

# LEARN: Health checks are standard in production systems. Kubernetes
# uses "liveness" and "readiness" probes to monitor services. Our
# health checks serve the same purpose for the data pipeline.
"""

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger("fittrack.monitor.health")


class HealthStatus:
    """Container for a single health check result."""

    def __init__(self, name: str, healthy: bool, message: str, details: dict[str, Any] | None = None):
        self.name = name
        self.healthy = healthy
        self.message = message
        self.details = details or {}
        self.checked_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "message": self.message,
            "details": self.details,
            "checked_at": self.checked_at,
        }


class HealthChecker:
    """
    Runs health checks against pipeline infrastructure.

    Usage:
        checker = HealthChecker(db_path="data/fittrack.duckdb")
        results = checker.run_all_checks()
        for r in results:
            status = "OK" if r.healthy else "FAIL"
            print(f"[{status}] {r.name}: {r.message}")
    """

    def __init__(
        self,
        db_path: str = "data/fittrack.duckdb",
        data_dir: str = "data",
        log_dir: str = "logs",
    ):
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.log_dir = Path(log_dir)

    def run_all_checks(self) -> list[HealthStatus]:
        """Run all health checks and return results."""
        checks = [
            self.check_storage(),
            self.check_database(),
            self.check_data_directories(),
            self.check_memory(),
            self.check_log_directory(),
        ]
        healthy_count = sum(1 for c in checks if c.healthy)
        logger.info(
            f"Health checks: {healthy_count}/{len(checks)} passed",
            extra={"layer": "monitor"},
        )
        return checks

    def check_storage(self) -> HealthStatus:
        """Check available disk space."""
        try:
            usage = shutil.disk_usage(str(self.data_dir.resolve()))
            free_gb = usage.free / (1024 ** 3)
            total_gb = usage.total / (1024 ** 3)
            used_pct = (usage.used / usage.total) * 100

            healthy = free_gb > 1.0  # At least 1 GB free
            return HealthStatus(
                name="disk_space",
                healthy=healthy,
                message=f"{free_gb:.1f} GB free of {total_gb:.1f} GB ({used_pct:.1f}% used)",
                details={
                    "free_gb": round(free_gb, 2),
                    "total_gb": round(total_gb, 2),
                    "used_pct": round(used_pct, 1),
                },
            )
        except Exception as e:
            return HealthStatus("disk_space", False, f"Check failed: {e}")

    def check_database(self) -> HealthStatus:
        """Check if DuckDB database is accessible."""
        try:
            import duckdb
            conn = duckdb.connect(str(self.db_path), read_only=True)
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchdf()
            conn.close()

            table_count = len(tables)
            return HealthStatus(
                name="database",
                healthy=True,
                message=f"DuckDB accessible: {table_count} tables",
                details={"table_count": table_count, "path": str(self.db_path)},
            )
        except FileNotFoundError:
            return HealthStatus(
                name="database",
                healthy=True,
                message="Database file not yet created (will be created on first run)",
                details={"path": str(self.db_path)},
            )
        except Exception as e:
            return HealthStatus("database", False, f"Database error: {e}")

    def check_data_directories(self) -> HealthStatus:
        """Check that required data directories exist."""
        required_dirs = ["data/bronze", "data/silver", "data/gold", "data/incoming"]
        missing = [d for d in required_dirs if not Path(d).exists()]

        if missing:
            return HealthStatus(
                name="data_directories",
                healthy=False,
                message=f"Missing directories: {', '.join(missing)}",
                details={"missing": missing},
            )
        return HealthStatus(
            name="data_directories",
            healthy=True,
            message="All data directories present",
            details={"checked": required_dirs},
        )

    def check_memory(self) -> HealthStatus:
        """Check system memory usage (basic check)."""
        try:
            import psutil
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 ** 3)
            used_pct = mem.percent
            return HealthStatus(
                name="memory",
                healthy=available_gb > 0.5,
                message=f"{available_gb:.1f} GB available ({used_pct}% used)",
                details={"available_gb": round(available_gb, 2), "used_pct": used_pct},
            )
        except ImportError:
            # psutil not installed — provide basic info
            return HealthStatus(
                name="memory",
                healthy=True,
                message="Memory check skipped (psutil not installed)",
            )
        except Exception as e:
            return HealthStatus("memory", False, f"Check failed: {e}")

    def check_log_directory(self) -> HealthStatus:
        """Check log directory health."""
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            log_files = list(self.log_dir.glob("*.log")) + list(self.log_dir.glob("*.json"))
            total_size_mb = sum(f.stat().st_size for f in log_files) / (1024 * 1024)

            return HealthStatus(
                name="log_directory",
                healthy=True,
                message=f"{len(log_files)} log files, {total_size_mb:.1f} MB total",
                details={
                    "file_count": len(log_files),
                    "total_size_mb": round(total_size_mb, 2),
                },
            )
        except Exception as e:
            return HealthStatus("log_directory", False, f"Check failed: {e}")
