"""
Pipeline Scheduler
===================

WHAT: Schedules pipeline runs using APScheduler with cron-like expressions.
Manages recurring execution of the full pipeline, quality checks, and
health monitoring.

WHY: Data pipelines need to run on schedule — daily ingestion, hourly
quality checks, periodic health monitoring. A scheduler automates this
so you don't have to manually trigger pipelines.

# LEARN: In production, scheduling is handled by:
#   - Airflow (most popular for data engineering)
#   - Cron (Linux/Mac built-in scheduler)
#   - Prefect / Dagster (modern alternatives)
#   - Azure Data Factory / AWS Step Functions (cloud-native)
# APScheduler gives us Airflow-like scheduling without the infrastructure.
# The cron expressions are identical across all these tools.
#
# Cron Expression Format: minute hour day_of_month month day_of_week
#   "0 6 * * *"     = Every day at 6:00 AM
#   "0 */4 * * *"   = Every 4 hours
#   "*/15 * * * *"  = Every 15 minutes
#   "0 6 * * 1-5"   = Weekdays at 6:00 AM
"""

import signal
import sys
from typing import Any

from src.utils.config_loader import load_pipeline_config
from src.utils.logger import get_logger

logger = get_logger("fittrack.monitor.scheduler")


class PipelineScheduler:
    """
    Manages scheduled execution of pipeline tasks.

    Usage:
        scheduler = PipelineScheduler()
        scheduler.start()  # Blocks and runs scheduled jobs
    """

    def __init__(self):
        self.config = load_pipeline_config()
        self._scheduler = None
        self._running = False

    def _setup_scheduler(self) -> Any:
        """Initialize APScheduler with configured jobs."""
        try:
            from apscheduler.schedulers.blocking import BlockingScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:
            logger.error("APScheduler not installed. Run: pip install apscheduler")
            return None

        scheduler = BlockingScheduler()
        sched_config = self.config.scheduling

        # Full pipeline run (default: daily at 6 AM)
        cron_parts = sched_config.full_pipeline_cron.split()
        if len(cron_parts) == 5:
            scheduler.add_job(
                self._run_full_pipeline,
                CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4],
                ),
                id="full_pipeline",
                name="Full Pipeline Run",
                misfire_grace_time=3600,
            )
            logger.info(f"Scheduled full pipeline: {sched_config.full_pipeline_cron}")

        # Quality checks (default: every 4 hours)
        cron_parts = sched_config.quality_check_cron.split()
        if len(cron_parts) == 5:
            scheduler.add_job(
                self._run_quality_checks,
                CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4],
                ),
                id="quality_checks",
                name="Quality Checks",
                misfire_grace_time=1800,
            )
            logger.info(f"Scheduled quality checks: {sched_config.quality_check_cron}")

        # Health checks (default: every 15 minutes)
        cron_parts = sched_config.health_check_cron.split()
        if len(cron_parts) == 5:
            scheduler.add_job(
                self._run_health_checks,
                CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4],
                ),
                id="health_checks",
                name="Health Checks",
                misfire_grace_time=300,
            )
            logger.info(f"Scheduled health checks: {sched_config.health_check_cron}")

        return scheduler

    def start(self) -> None:
        """Start the scheduler (blocking)."""
        if not self.config.scheduling.enabled:
            logger.warning("Scheduling is disabled in config. " "Set scheduling.enabled: true to activate.")
            return

        self._scheduler = self._setup_scheduler()
        if self._scheduler is None:
            return

        # Handle graceful shutdown
        def _signal_handler(signum: int, frame: Any) -> None:
            logger.info("Shutdown signal received. Stopping scheduler...")
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
            sys.exit(0)

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        logger.info("Pipeline scheduler started. Press Ctrl+C to stop.")
        self._running = True

        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped.")
            self._running = False

    @staticmethod
    def _run_full_pipeline() -> None:
        """Execute the full pipeline (called by scheduler)."""
        logger.info("Scheduler triggered: Full Pipeline Run")
        try:
            from src.orchestrator import PipelineOrchestrator

            orchestrator = PipelineOrchestrator()
            orchestrator.run_full_pipeline()
        except Exception as e:
            logger.error(f"Scheduled pipeline run failed: {e}", exc_info=True)

    @staticmethod
    def _run_quality_checks() -> None:
        """Execute quality checks only (called by scheduler)."""
        logger.info("Scheduler triggered: Quality Checks")
        try:
            from src.orchestrator import PipelineOrchestrator

            orchestrator = PipelineOrchestrator()
            orchestrator.run_quality_only()
        except Exception as e:
            logger.error(f"Scheduled quality checks failed: {e}", exc_info=True)

    @staticmethod
    def _run_health_checks() -> None:
        """Execute health checks (called by scheduler)."""
        logger.info("Scheduler triggered: Health Checks")
        try:
            from src.monitor.alerter import Alerter
            from src.monitor.health_check import HealthChecker

            checker = HealthChecker()
            results = checker.run_all_checks()

            unhealthy = [r for r in results if not r.healthy]
            if unhealthy:
                alerter = Alerter()
                for r in unhealthy:
                    alerter.alert(
                        "WARNING",
                        "health_check",
                        f"{r.name}: {r.message}",
                    )
        except Exception as e:
            logger.error(f"Scheduled health check failed: {e}", exc_info=True)
