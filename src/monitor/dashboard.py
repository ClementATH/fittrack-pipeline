"""
FitTrack Pro ETL Pipeline — Monitoring Dashboard
==================================================

WHAT: A Streamlit dashboard showing pipeline health, data quality trends,
run history, and recent alerts at a glance.

WHY: Dashboards give you real-time visibility into your data platform.
Without one, you're flying blind — you only discover issues when someone
complains about bad data in a report.

# LEARN: Every production data platform has a monitoring dashboard.
# At WellMed, your team likely uses Datadog, Grafana, or a custom
# Snowflake dashboard. The concepts are the same:
#   - Pipeline run history (did today's runs succeed?)
#   - Data quality trends (is quality improving or degrading?)
#   - System health (is the infrastructure healthy?)
#   - Alerts (what needs attention right now?)

Run with:
    streamlit run src/monitor/dashboard.py --server.port 8501
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Resolve project root so imports work when Streamlit runs from any directory
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitor.health_check import HealthChecker  # noqa: E402

# ============================================================
# Page config
# ============================================================
st.set_page_config(
    page_title="FitTrack ETL Dashboard",
    page_icon="💪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# Helper functions
# ============================================================


def load_pipeline_runs() -> pd.DataFrame:
    """Load pipeline run history from the DuckDB database."""
    db_path = PROJECT_ROOT / "data" / "fittrack.duckdb"
    if not db_path.exists():
        return pd.DataFrame(
            columns=[
                "run_id",
                "pipeline_name",
                "source_name",
                "layer",
                "status",
                "started_at",
                "completed_at",
                "rows_processed",
                "rows_failed",
            ]
        )
    try:
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            df = conn.execute("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 100").fetchdf()
        except duckdb.CatalogException:
            df = pd.DataFrame()
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def load_quality_scores() -> pd.DataFrame:
    """Load quality scores from DuckDB."""
    db_path = PROJECT_ROOT / "data" / "fittrack.duckdb"
    if not db_path.exists():
        return pd.DataFrame()
    try:
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            df = conn.execute("SELECT * FROM quality_scores ORDER BY scored_at DESC LIMIT 200").fetchdf()
        except duckdb.CatalogException:
            df = pd.DataFrame()
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def load_alerts() -> list[dict]:
    """Load recent alerts from the JSON log."""
    alert_path = PROJECT_ROOT / "logs" / "alerts.json"
    if not alert_path.exists():
        return []
    try:
        content = alert_path.read_text(encoding="utf-8")
        return json.loads(content) if content.strip() else []
    except (json.JSONDecodeError, Exception):
        return []


def count_gold_files() -> dict[str, int]:
    """Count Parquet files in each data layer."""
    counts: dict[str, int] = {}
    for layer in ("bronze", "silver", "gold"):
        layer_dir = PROJECT_ROOT / "data" / layer
        if layer_dir.exists():
            counts[layer] = len(list(layer_dir.rglob("*.parquet")))
        else:
            counts[layer] = 0
    return counts


# ============================================================
# Sidebar
# ============================================================

with st.sidebar:
    st.title("💪 FitTrack ETL")
    st.caption("Pipeline Monitoring Dashboard")
    st.divider()

    page = st.radio(
        "Navigate",
        ["Overview", "Pipeline Runs", "Data Quality", "Health Checks", "Alerts"],
        index=0,
    )

    st.divider()
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh"):
        st.rerun()


# ============================================================
# PAGE: Overview
# ============================================================
if page == "Overview":
    st.title("📊 Pipeline Overview")
    st.markdown("Real-time status of the FitTrack Pro ETL pipeline.")

    # Top metrics row
    runs_df = load_pipeline_runs()
    alerts = load_alerts()
    file_counts = count_gold_files()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_runs = len(runs_df) if not runs_df.empty else 0
        st.metric("Total Pipeline Runs", total_runs)
    with col2:
        success_runs = (
            len(runs_df[runs_df["status"] == "success"]) if not runs_df.empty and "status" in runs_df.columns else 0
        )
        st.metric("Successful Runs", success_runs)
    with col3:
        st.metric("Active Alerts", len(alerts))
    with col4:
        total_files = sum(file_counts.values())
        st.metric("Data Files", total_files)

    st.divider()

    # Data layer overview
    st.subheader("Data Layer Status")
    layer_col1, layer_col2, layer_col3 = st.columns(3)
    with layer_col1:
        st.metric("🥉 Bronze (Raw)", f"{file_counts.get('bronze', 0)} files")
    with layer_col2:
        st.metric("🥈 Silver (Clean)", f"{file_counts.get('silver', 0)} files")
    with layer_col3:
        st.metric("🥇 Gold (Business)", f"{file_counts.get('gold', 0)} files")

    st.divider()

    # Recent pipeline runs
    st.subheader("Recent Pipeline Runs")
    if not runs_df.empty:
        display_cols = [
            c
            for c in ["pipeline_name", "source_name", "layer", "status", "started_at", "rows_processed"]
            if c in runs_df.columns
        ]
        st.dataframe(runs_df[display_cols].head(10), use_container_width=True)
    else:
        st.info("No pipeline runs recorded yet. Run the pipeline to see data here.")

    # Recent alerts summary
    st.subheader("Recent Alerts")
    if alerts:
        for alert in alerts[-5:]:
            severity = alert.get("severity", "INFO")
            icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(severity, "⚪")
            st.markdown(
                f"{icon} **[{severity}]** {alert.get('source', '?')}: "
                f"{alert.get('message', '')} — "
                f"*{alert.get('timestamp', '')[:19]}*"
            )
    else:
        st.success("No alerts. Everything looks good!")


# ============================================================
# PAGE: Pipeline Runs
# ============================================================
elif page == "Pipeline Runs":
    st.title("🔄 Pipeline Run History")

    runs_df = load_pipeline_runs()

    if runs_df.empty:
        st.info("No pipeline runs recorded yet. Run the pipeline to populate this page.")
    else:
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            if "status" in runs_df.columns:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=runs_df["status"].unique().tolist(),
                    default=runs_df["status"].unique().tolist(),
                )
                runs_df = runs_df[runs_df["status"].isin(status_filter)]
        with col2:
            if "layer" in runs_df.columns:
                layer_filter = st.multiselect(
                    "Filter by Layer",
                    options=runs_df["layer"].unique().tolist(),
                    default=runs_df["layer"].unique().tolist(),
                )
                runs_df = runs_df[runs_df["layer"].isin(layer_filter)]

        st.dataframe(runs_df, use_container_width=True)

        # Run success rate chart
        if "status" in runs_df.columns:
            st.subheader("Run Status Distribution")
            status_counts = runs_df["status"].value_counts()
            st.bar_chart(status_counts)


# ============================================================
# PAGE: Data Quality
# ============================================================
elif page == "Data Quality":
    st.title("✅ Data Quality Scores")

    scores_df = load_quality_scores()

    if scores_df.empty:
        st.info("No quality scores recorded yet. Run the quality checks to see scores here.")
    else:
        # Latest scores per table
        st.subheader("Latest Quality Scores")

        if "table_name" in scores_df.columns and "overall_score" in scores_df.columns:
            latest = scores_df.drop_duplicates(subset=["table_name"], keep="first")

            for _, row in latest.iterrows():
                score = row.get("overall_score", 0)
                table = row.get("table_name", "unknown")
                color = "green" if score >= 80 else ("orange" if score >= 60 else "red")

                col1, col2 = st.columns([3, 1])
                with col1:
                    st.progress(min(score / 100, 1.0), text=f"{table}: {score:.1f}/100")
                with col2:
                    grade = (
                        "A+"
                        if score >= 95
                        else "A"
                        if score >= 90
                        else "B+"
                        if score >= 85
                        else "B"
                        if score >= 80
                        else "C"
                        if score >= 70
                        else "D"
                        if score >= 60
                        else "F"
                    )
                    st.markdown(f"**Grade: {grade}**")

        # Quality dimensions breakdown
        st.subheader("Quality Dimensions")
        dim_cols = ["table_name", "completeness_score", "accuracy_score", "consistency_score", "timeliness_score"]
        available = [c for c in dim_cols if c in scores_df.columns]
        if len(available) > 1:
            st.dataframe(
                scores_df[available].drop_duplicates(subset=["table_name"], keep="first"),
                use_container_width=True,
            )

    # Quality reports
    st.subheader("Generated Reports")
    reports_dir = PROJECT_ROOT / "reports"
    if reports_dir.exists():
        reports = sorted(reports_dir.glob("*.md"), reverse=True)
        if reports:
            for report in reports[:10]:
                with st.expander(f"📄 {report.name}"):
                    st.markdown(report.read_text(encoding="utf-8")[:3000])
        else:
            st.info("No quality reports generated yet.")
    else:
        st.info("Reports directory not found.")


# ============================================================
# PAGE: Health Checks
# ============================================================
elif page == "Health Checks":
    st.title("🏥 System Health")

    if st.button("Run Health Checks Now"):
        with st.spinner("Running health checks..."):
            checker = HealthChecker(
                db_path=str(PROJECT_ROOT / "data" / "fittrack.duckdb"),
                data_dir=str(PROJECT_ROOT / "data"),
                log_dir=str(PROJECT_ROOT / "logs"),
            )
            results = checker.run_all_checks()

        for r in results:
            icon = "✅" if r.healthy else "❌"
            with st.container():
                col1, col2 = st.columns([1, 4])
                with col1:
                    st.markdown(f"### {icon}")
                with col2:
                    st.markdown(f"**{r.name}**")
                    st.caption(r.message)
                    if r.details:
                        st.json(r.details)

        healthy_count = sum(1 for r in results if r.healthy)
        total = len(results)
        if healthy_count == total:
            st.success(f"All {total} health checks passed!")
        else:
            st.warning(f"{healthy_count}/{total} health checks passed.")
    else:
        st.info("Click the button above to run health checks.")


# ============================================================
# PAGE: Alerts
# ============================================================
elif page == "Alerts":
    st.title("🚨 Alerts")

    alerts = load_alerts()

    if not alerts:
        st.success("No alerts recorded. System is running smoothly!")
    else:
        # Alert counts
        col1, col2, col3 = st.columns(3)
        critical = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
        warning = sum(1 for a in alerts if a.get("severity") == "WARNING")
        info = sum(1 for a in alerts if a.get("severity") == "INFO")

        col1.metric("🔴 Critical", critical)
        col2.metric("🟡 Warning", warning)
        col3.metric("🔵 Info", info)

        st.divider()

        # Severity filter
        severity_filter = st.multiselect(
            "Filter by severity",
            options=["CRITICAL", "WARNING", "INFO"],
            default=["CRITICAL", "WARNING", "INFO"],
        )

        filtered = [a for a in alerts if a.get("severity") in severity_filter]

        # Display alerts (newest first)
        for alert in reversed(filtered[-50:]):
            severity = alert.get("severity", "INFO")
            icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(severity, "⚪")

            with st.expander(f"{icon} [{severity}] {alert.get('source', '?')}: " f"{alert.get('message', '')[:80]}"):
                st.markdown(f"**Time:** {alert.get('timestamp', 'N/A')}")
                st.markdown(f"**Source:** {alert.get('source', 'N/A')}")
                st.markdown(f"**Message:** {alert.get('message', 'N/A')}")
                if alert.get("details"):
                    st.json(alert["details"])
