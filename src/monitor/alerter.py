"""
Pipeline Alerter
=================

WHAT: Logs alerts with severity levels to a JSON file and console.
Provides a centralized alerting interface for the entire pipeline.

WHY: When something goes wrong at 2 AM, alerts are how you find out.
Every pipeline failure, quality issue, or health check failure
generates an alert that can be reviewed in the dashboard.

# LEARN: In production, alerts would go to Slack, PagerDuty, or email.
# For this project, we log to a JSON file that the dashboard reads.
# The pattern is the same — a centralized alert bus that multiple
# consumers can subscribe to.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.logger import get_logger

logger = get_logger("fittrack.monitor.alerter")


class Alert:
    """Represents a single alert."""

    def __init__(
        self,
        severity: str,
        source: str,
        message: str,
        details: dict[str, Any] | None = None,
    ):
        self.id = str(uuid.uuid4())
        self.severity = severity  # CRITICAL, WARNING, INFO
        self.source = source
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.acknowledged = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "severity": self.severity,
            "source": self.source,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
            "acknowledged": self.acknowledged,
        }


class Alerter:
    """
    Centralized alerting system for the pipeline.

    Usage:
        alerter = Alerter("logs/alerts.json")
        alerter.alert("WARNING", "ingestion", "API rate limited", {"endpoint": "/exercises"})
        recent = alerter.get_recent_alerts(limit=10)
    """

    def __init__(self, alert_log_path: str = "logs/alerts.json"):
        self.alert_log_path = Path(alert_log_path)
        self.alert_log_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize file if it doesn't exist
        if not self.alert_log_path.exists():
            self.alert_log_path.write_text("[]", encoding="utf-8")

    def alert(
        self,
        severity: str,
        source: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> Alert:
        """Create and store a new alert."""
        alert_obj = Alert(severity, source, message, details)

        # Log to Python logger
        log_method = {
            "CRITICAL": logger.critical,
            "WARNING": logger.warning,
            "INFO": logger.info,
        }.get(severity, logger.info)
        log_method(f"[{source}] {message}", extra={"layer": "monitor"})

        # Append to JSON file
        self._append_alert(alert_obj)

        return alert_obj

    def _append_alert(self, alert_obj: Alert) -> None:
        """Append an alert to the JSON log file."""
        try:
            alerts = self._load_alerts()
            alerts.append(alert_obj.to_dict())

            # Keep only last 1000 alerts to prevent file growth
            if len(alerts) > 1000:
                alerts = alerts[-1000:]

            self.alert_log_path.write_text(
                json.dumps(alerts, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            logger.error(f"Failed to write alert: {e}")

    def _load_alerts(self) -> list[dict[str, Any]]:
        """Load all alerts from the JSON file."""
        try:
            if self.alert_log_path.exists():
                content = self.alert_log_path.read_text(encoding="utf-8")
                if content.strip():
                    return json.loads(content)
            return []
        except (json.JSONDecodeError, Exception):
            return []

    def get_recent_alerts(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get the most recent alerts."""
        alerts = self._load_alerts()
        return alerts[-limit:]

    def get_alerts_by_severity(self, severity: str) -> list[dict[str, Any]]:
        """Get alerts filtered by severity."""
        alerts = self._load_alerts()
        return [a for a in alerts if a.get("severity") == severity]

    def get_alert_counts(self) -> dict[str, int]:
        """Get count of alerts by severity."""
        alerts = self._load_alerts()
        counts = {"CRITICAL": 0, "WARNING": 0, "INFO": 0}
        for a in alerts:
            sev = a.get("severity", "INFO")
            counts[sev] = counts.get(sev, 0) + 1
        return counts
