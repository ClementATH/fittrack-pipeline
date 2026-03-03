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
import plotly.graph_objects as go
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
# Custom CSS injection
# ============================================================
def inject_custom_css() -> None:
    """Inject custom CSS for dark glassmorphism theme with animations."""
    st.markdown(
        """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ── Glassmorphism card ── */
    .glass-card {
        background: rgba(255, 255, 255, 0.04);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .glass-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(108, 99, 255, 0.12);
    }

    /* ── Metric card ── */
    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 14px;
        padding: 1.25rem 1.5rem;
        border-left: 4px solid #6C63FF;
        transition: all 0.3s ease;
        min-height: 120px;
    }
    .metric-card:hover {
        background: rgba(255, 255, 255, 0.06);
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(108, 99, 255, 0.1);
    }
    .metric-card .metric-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #FAFAFA;
        line-height: 1.2;
        margin-top: 0.25rem;
    }
    .metric-card .metric-label {
        font-size: 0.8rem;
        color: rgba(250, 250, 250, 0.5);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 600;
    }
    .metric-card .metric-icon {
        font-size: 1.4rem;
        margin-bottom: 0.5rem;
    }
    .metric-card.success { border-left-color: #00D68F; }
    .metric-card.warning { border-left-color: #FFAA00; }
    .metric-card.danger  { border-left-color: #FF3D71; }
    .metric-card.info    { border-left-color: #0095FF; }

    /* ── Status badges ── */
    .status-badge {
        display: inline-block;
        padding: 0.2rem 0.7rem;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .status-badge.success { background: rgba(0, 214, 143, 0.15); color: #00D68F; }
    .status-badge.fail    { background: rgba(255, 61, 113, 0.15); color: #FF3D71; }
    .status-badge.warning { background: rgba(255, 170, 0, 0.15);  color: #FFAA00; }
    .status-badge.blocked { background: rgba(150, 150, 150, 0.15); color: #999; }
    .status-badge.info    { background: rgba(0, 149, 255, 0.15);  color: #0095FF; }

    /* ── Section header ── */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: rgba(250, 250, 250, 0.7);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }

    /* ── Data table styling ── */
    .stDataFrame tbody tr:nth-child(even) {
        background-color: rgba(255, 255, 255, 0.02);
    }
    .stDataFrame tbody tr:hover {
        background-color: rgba(108, 99, 255, 0.08) !important;
    }

    /* ── Fade-in animation ── */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(16px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    .main .block-container {
        animation: fadeInUp 0.4s ease-out;
    }

    /* ── Sidebar styling ── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #131620 100%);
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }

    /* ── Hide Streamlit defaults ── */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header { visibility: hidden; }

    /* ── Health check card ── */
    .health-card {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 0.75rem;
        border-left: 4px solid #00D68F;
        transition: all 0.2s ease;
    }
    .health-card:hover {
        background: rgba(255, 255, 255, 0.05);
    }
    .health-card.unhealthy {
        border-left-color: #FF3D71;
    }
    .health-card .health-name {
        font-weight: 600;
        font-size: 1rem;
        color: #FAFAFA;
    }
    .health-card .health-msg {
        font-size: 0.85rem;
        color: rgba(250, 250, 250, 0.6);
        margin-top: 0.25rem;
    }

    /* ── Alert item ── */
    .alert-item {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 12px;
        padding: 1rem 1.25rem;
        margin-bottom: 0.5rem;
        border-left: 4px solid #6C63FF;
        transition: all 0.2s ease;
    }
    .alert-item:hover {
        background: rgba(255, 255, 255, 0.05);
    }
    .alert-item.critical { border-left-color: #FF3D71; }
    .alert-item.warning  { border-left-color: #FFAA00; }
    .alert-item.info     { border-left-color: #0095FF; }

    /* ── Page title styling ── */
    .page-title {
        font-size: 1.8rem;
        font-weight: 700;
        color: #FAFAFA;
        margin-bottom: 0.25rem;
    }
    .page-subtitle {
        font-size: 0.95rem;
        color: rgba(250, 250, 250, 0.5);
        margin-bottom: 1.5rem;
    }

    /* ── Empty state ── */
    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        color: rgba(250, 250, 250, 0.4);
    }
    .empty-state .empty-icon {
        font-size: 3rem;
        margin-bottom: 1rem;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


inject_custom_css()


# ============================================================
# Plotly theme defaults
# ============================================================
def get_plotly_defaults() -> dict:
    """Return base Plotly layout settings for dark theme consistency."""
    return dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#FAFAFA", size=13),
        margin=dict(l=40, r=20, t=50, b=40),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", zerolinecolor="rgba(255,255,255,0.05)"),
        hoverlabel=dict(bgcolor="#1A1D29", font_size=13, font_family="Inter, sans-serif"),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#FAFAFA")),
    )


PLOTLY_CONFIG = {"displayModeBar": False}


# ============================================================
# Component helpers
# ============================================================
def render_metric_card(icon: str, label: str, value, variant: str = "") -> None:
    """Render a styled metric card with glassmorphism effect."""
    variant_class = f" {variant}" if variant else ""
    st.markdown(
        f"""
    <div class="metric-card{variant_class}">
        <div class="metric-icon">{icon}</div>
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_status_badge(status: str) -> str:
    """Return HTML for a colored status pill badge."""
    badge_map = {
        "success": "success",
        "pass": "success",
        "healthy": "success",
        "loaded": "success",
        "failure": "fail",
        "fail": "fail",
        "error": "fail",
        "critical": "fail",
        "warning": "warning",
        "blocked": "blocked",
    }
    css_class = badge_map.get(status.lower(), "info")
    return f'<span class="status-badge {css_class}">{status}</span>'


def render_section_header(text: str) -> None:
    """Render a styled section divider with label."""
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


def render_empty_state(icon: str, message: str) -> None:
    """Render a centered empty state placeholder."""
    st.markdown(
        f"""
    <div class="glass-card empty-state">
        <div class="empty-icon">{icon}</div>
        <p>{message}</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


# ============================================================
# Chart factories
# ============================================================
def create_status_distribution_chart(runs_df: pd.DataFrame) -> go.Figure:
    """Create an animated bar chart showing run status distribution."""
    if runs_df.empty or "status" not in runs_df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No data", showarrow=False, font=dict(size=16, color="rgba(250,250,250,0.3)"))
        fig.update_layout(**get_plotly_defaults(), height=250)
        return fig

    status_counts = runs_df["status"].value_counts()
    colors = {"success": "#00D68F", "failure": "#FF3D71", "error": "#FF3D71", "blocked": "#999", "running": "#0095FF"}
    bar_colors = [colors.get(s, "#6C63FF") for s in status_counts.index]

    fig = go.Figure(
        go.Bar(
            x=status_counts.index,
            y=status_counts.values,
            marker=dict(color=bar_colors, line=dict(width=0), cornerradius=6),
            text=status_counts.values,
            textposition="outside",
            textfont=dict(color="#FAFAFA", size=14, family="Inter"),
            hovertemplate="<b>%{x}</b><br>Count: %{y}<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=300,
        title=dict(text="Status Distribution", font=dict(size=16)),
        showlegend=False,
        transition=dict(duration=500, easing="cubic-in-out"),
    )
    return fig


def create_pipeline_timeline(runs_df: pd.DataFrame) -> go.Figure:
    """Create a scatter timeline showing pipeline runs over time."""
    if runs_df.empty or "started_at" not in runs_df.columns:
        fig = go.Figure()
        fig.add_annotation(text="No data", showarrow=False, font=dict(size=16, color="rgba(250,250,250,0.3)"))
        fig.update_layout(**get_plotly_defaults(), height=280)
        return fig

    status_colors = {"success": "#00D68F", "failure": "#FF3D71", "error": "#FF3D71", "blocked": "#999"}

    fig = go.Figure()
    for status in runs_df["status"].unique():
        subset = runs_df[runs_df["status"] == status]
        sizes = (
            subset["rows_processed"].clip(lower=8).apply(lambda x: min(x, 35))
            if "rows_processed" in subset.columns
            else [12] * len(subset)
        )
        fig.add_trace(
            go.Scatter(
                x=subset["started_at"],
                y=subset.get("source_name", subset.get("pipeline_name", ["Pipeline"] * len(subset))),
                mode="markers",
                name=status.title(),
                marker=dict(
                    size=sizes,
                    color=status_colors.get(status, "#6C63FF"),
                    line=dict(width=1, color="rgba(255,255,255,0.15)"),
                    opacity=0.85,
                ),
                hovertemplate="<b>%{y}</b><br>Time: %{x}<br>Status: " + status + "<extra></extra>",
            )
        )

    fig.update_layout(
        **get_plotly_defaults(),
        height=280,
        title=dict(text="Run Timeline", font=dict(size=16)),
        xaxis_title="",
        yaxis_title="",
    )
    return fig


def create_quality_gauge(score: float, table_name: str) -> go.Figure:
    """Create an animated gauge chart for a quality score."""
    color = "#00D68F" if score >= 80 else ("#FFAA00" if score >= 60 else "#FF3D71")

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number=dict(suffix="/100", font=dict(size=26, color="#FAFAFA")),
            title=dict(text=table_name.replace("_", " ").title(), font=dict(size=14, color="rgba(250,250,250,0.7)")),
            gauge=dict(
                axis=dict(range=[0, 100], tickcolor="rgba(255,255,255,0.2)", tickfont=dict(size=10)),
                bar=dict(color=color, thickness=0.8),
                bgcolor="rgba(255,255,255,0.03)",
                borderwidth=0,
                steps=[
                    dict(range=[0, 60], color="rgba(255, 61, 113, 0.06)"),
                    dict(range=[60, 80], color="rgba(255, 170, 0, 0.06)"),
                    dict(range=[80, 100], color="rgba(0, 214, 143, 0.06)"),
                ],
                threshold=dict(line=dict(color="#FAFAFA", width=2), thickness=0.75, value=score),
            ),
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#FAFAFA"),
        height=220,
        margin=dict(l=20, r=20, t=55, b=15),
    )
    return fig


def create_quality_radar(scores_row: pd.Series) -> go.Figure:
    """Create a radar chart showing 4 quality dimensions."""
    categories = ["Completeness", "Accuracy", "Consistency", "Timeliness"]
    values = [
        float(scores_row.get("completeness_score", 0)),
        float(scores_row.get("accuracy_score", 0)),
        float(scores_row.get("consistency_score", 0)),
        float(scores_row.get("timeliness_score", 0)),
    ]
    values.append(values[0])
    categories.append(categories[0])

    fig = go.Figure(
        go.Scatterpolar(
            r=values,
            theta=categories,
            fill="toself",
            fillcolor="rgba(108, 99, 255, 0.15)",
            line=dict(color="#6C63FF", width=2),
            marker=dict(size=6, color="#6C63FF"),
        )
    )
    fig.update_layout(
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                gridcolor="rgba(255,255,255,0.08)",
                tickfont=dict(size=9, color="rgba(250,250,250,0.4)"),
            ),
            angularaxis=dict(gridcolor="rgba(255,255,255,0.08)", tickfont=dict(size=11, color="rgba(250,250,250,0.7)")),
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#FAFAFA"),
        showlegend=False,
        height=280,
        margin=dict(l=50, r=50, t=30, b=30),
    )
    return fig


def create_severity_donut(alerts: list[dict]) -> go.Figure:
    """Create a donut chart showing alert severity distribution."""
    counts = {"CRITICAL": 0, "WARNING": 0, "INFO": 0}
    for a in alerts:
        sev = a.get("severity", "INFO")
        counts[sev] = counts.get(sev, 0) + 1

    labels = list(counts.keys())
    values = list(counts.values())
    colors = ["#FF3D71", "#FFAA00", "#0095FF"]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.65,
            marker=dict(colors=colors, line=dict(color="#0E1117", width=3)),
            textinfo="label+value",
            textfont=dict(size=12, color="#FAFAFA"),
            hovertemplate="<b>%{label}</b><br>Count: %{value}<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#FAFAFA"),
        showlegend=False,
        height=250,
        margin=dict(l=20, r=20, t=20, b=20),
        annotations=[
            dict(text="Alerts", x=0.5, y=0.5, font_size=16, font_color="rgba(250,250,250,0.5)", showarrow=False)
        ],
    )
    return fig


def create_layer_bar(file_counts: dict[str, int]) -> go.Figure:
    """Create a horizontal bar chart showing data layer file counts."""
    layers = ["Bronze", "Silver", "Gold"]
    counts = [file_counts.get("bronze", 0), file_counts.get("silver", 0), file_counts.get("gold", 0)]
    colors = ["#CD7F32", "#C0C0C0", "#FFD700"]

    fig = go.Figure(
        go.Bar(
            y=layers,
            x=counts,
            orientation="h",
            marker=dict(color=colors, line=dict(width=0), cornerradius=4),
            text=counts,
            textposition="outside",
            textfont=dict(color="#FAFAFA", size=13),
            hovertemplate="<b>%{y}</b><br>Files: %{x}<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=180,
        showlegend=False,
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
        margin=dict(l=60, r=40, t=10, b=10),
    )
    return fig


# ============================================================
# Data loaders (unchanged logic)
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


def get_grade(score: float) -> str:
    """Return letter grade for a quality score."""
    if score >= 95:
        return "A+"
    elif score >= 90:
        return "A"
    elif score >= 85:
        return "B+"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    elif score >= 60:
        return "D"
    return "F"


# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.markdown(
        """
    <div style="text-align: center; padding: 1.5rem 0 0.5rem 0;">
        <span style="font-size: 2.5rem;">💪</span>
        <h2 style="margin: 0.25rem 0 0; font-weight: 700;
            background: linear-gradient(135deg, #6C63FF, #00D68F);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            font-size: 1.5rem;">
            FitTrack ETL
        </h2>
        <p style="color: rgba(250,250,250,0.4); font-size: 0.8rem; margin: 0.25rem 0 0;">
            Pipeline Monitoring
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.divider()

    page = st.radio(
        "Navigate",
        ["Overview", "Pipeline Runs", "Data Quality", "Health Checks", "Alerts"],
        index=0,
        label_visibility="collapsed",
    )

    st.divider()

    st.caption(f"v1.0.0 · {datetime.now().strftime('%H:%M:%S')}")
    if st.button("↻ Refresh Data", use_container_width=True):
        st.rerun()


# ============================================================
# PAGE: Overview
# ============================================================
if page == "Overview":
    st.markdown('<div class="page-title">Pipeline Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Real-time status of the FitTrack Pro ETL pipeline</div>', unsafe_allow_html=True
    )

    runs_df = load_pipeline_runs()
    alerts = load_alerts()
    file_counts = count_gold_files()

    total_runs = len(runs_df) if not runs_df.empty else 0
    success_runs = (
        len(runs_df[runs_df["status"] == "success"]) if not runs_df.empty and "status" in runs_df.columns else 0
    )
    total_files = sum(file_counts.values())

    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_metric_card("🔄", "Total Runs", total_runs, "info")
    with col2:
        render_metric_card("✅", "Successful", success_runs, "success")
    with col3:
        render_metric_card("🚨", "Active Alerts", len(alerts), "danger" if alerts else "")
    with col4:
        render_metric_card("📁", "Data Files", total_files)

    # Data layer status
    render_section_header("Data Layer Status")
    col_chart, col_metrics = st.columns([2, 3])
    with col_chart:
        st.plotly_chart(create_layer_bar(file_counts), use_container_width=True, config=PLOTLY_CONFIG)
    with col_metrics:
        lc1, lc2, lc3 = st.columns(3)
        with lc1:
            render_metric_card("🥉", "Bronze (Raw)", f"{file_counts.get('bronze', 0)}")
        with lc2:
            render_metric_card("🥈", "Silver (Clean)", f"{file_counts.get('silver', 0)}")
        with lc3:
            render_metric_card("🥇", "Gold (Business)", f"{file_counts.get('gold', 0)}")

    # Recent pipeline runs
    render_section_header("Recent Pipeline Runs")
    if not runs_df.empty:
        display_cols = [
            c
            for c in ["pipeline_name", "source_name", "layer", "status", "started_at", "rows_processed"]
            if c in runs_df.columns
        ]
        st.dataframe(runs_df[display_cols].head(10), use_container_width=True, hide_index=True)
    else:
        render_empty_state("📊", "No pipeline runs recorded yet. Run the pipeline to see data here.")

    # Recent alerts
    render_section_header("Recent Alerts")
    if alerts:
        for alert in alerts[-5:]:
            severity = alert.get("severity", "INFO")
            sev_class = {"CRITICAL": "critical", "WARNING": "warning", "INFO": "info"}.get(severity, "info")
            st.markdown(
                f"""
            <div class="alert-item {sev_class}">
                {render_status_badge(severity)}
                <span style="margin-left: 0.75rem; color: rgba(250,250,250,0.8);">
                    <strong>{alert.get('source', '?')}</strong>: {alert.get('message', '')}
                </span>
                <span style="float: right; color: rgba(250,250,250,0.35); font-size: 0.8rem;">
                    {alert.get('timestamp', '')[:19]}
                </span>
            </div>
            """,
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            """
        <div class="glass-card" style="text-align: center; border-left: 4px solid #00D68F;">
            <span style="font-size: 1.5rem;">✅</span>
            <span style="margin-left: 0.5rem; color: #00D68F; font-weight: 600;">No alerts — everything looks good!</span>
        </div>
        """,
            unsafe_allow_html=True,
        )


# ============================================================
# PAGE: Pipeline Runs
# ============================================================
elif page == "Pipeline Runs":
    st.markdown('<div class="page-title">Pipeline Run History</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Track every pipeline execution across all data sources</div>',
        unsafe_allow_html=True,
    )

    runs_df = load_pipeline_runs()

    if runs_df.empty:
        render_empty_state("🔄", "No pipeline runs recorded yet. Run the pipeline to populate this page.")
    else:
        # Filters in a glass card
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
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
        st.markdown("</div>", unsafe_allow_html=True)

        # Timeline + status chart
        render_section_header("Visualizations")
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(create_pipeline_timeline(runs_df), use_container_width=True, config=PLOTLY_CONFIG)
        with chart_col2:
            st.plotly_chart(create_status_distribution_chart(runs_df), use_container_width=True, config=PLOTLY_CONFIG)

        # Full data table
        render_section_header("Run Details")
        st.dataframe(runs_df, use_container_width=True, hide_index=True)


# ============================================================
# PAGE: Data Quality
# ============================================================
elif page == "Data Quality":
    st.markdown('<div class="page-title">Data Quality Scores</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Quality assessment across all pipeline datasets</div>', unsafe_allow_html=True
    )

    scores_df = load_quality_scores()

    if scores_df.empty:
        render_empty_state("✅", "No quality scores recorded yet. Run the quality checks to see scores here.")
    else:
        if "table_name" in scores_df.columns and "overall_score" in scores_df.columns:
            latest = scores_df.drop_duplicates(subset=["table_name"], keep="first")

            # Gauge charts
            render_section_header("Overall Scores")
            gauge_cols = st.columns(len(latest))
            for i, (_, row) in enumerate(latest.iterrows()):
                score = row.get("overall_score", 0)
                table = row.get("table_name", "unknown")
                with gauge_cols[i]:
                    st.plotly_chart(create_quality_gauge(score, table), use_container_width=True, config=PLOTLY_CONFIG)
                    grade = get_grade(score)
                    color = "#00D68F" if score >= 80 else ("#FFAA00" if score >= 60 else "#FF3D71")
                    st.markdown(
                        f'<div style="text-align: center; margin-top: -10px;">'
                        f'<span style="font-size: 1.4rem; font-weight: 700; color: {color};">{grade}</span>'
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            # Radar charts
            render_section_header("Quality Dimensions")
            dim_cols_needed = ["completeness_score", "accuracy_score", "consistency_score", "timeliness_score"]
            has_dims = all(c in scores_df.columns for c in dim_cols_needed)

            if has_dims:
                radar_cols = st.columns(min(len(latest), 4))
                for i, (_, row) in enumerate(latest.iterrows()):
                    with radar_cols[i % len(radar_cols)]:
                        table = row.get("table_name", "unknown")
                        st.markdown(
                            f'<div style="text-align: center; font-weight: 600; color: rgba(250,250,250,0.7); margin-bottom: 0.25rem;">'
                            f'{table.replace("_", " ").title()}</div>',
                            unsafe_allow_html=True,
                        )
                        st.plotly_chart(create_quality_radar(row), use_container_width=True, config=PLOTLY_CONFIG)

            # Dimensions table with heatmap
            render_section_header("Score Breakdown")
            dim_display = ["table_name", *dim_cols_needed]
            available = [c for c in dim_display if c in scores_df.columns]
            if len(available) > 1:
                display_data = scores_df[available].drop_duplicates(subset=["table_name"], keep="first")
                score_cols = [c for c in dim_cols_needed if c in display_data.columns]
                styled = display_data.style.background_gradient(cmap="RdYlGn", subset=score_cols, vmin=0, vmax=100)
                st.dataframe(styled, use_container_width=True, hide_index=True)

    # Quality reports
    render_section_header("Generated Reports")
    reports_dir = PROJECT_ROOT / "reports"
    if reports_dir.exists():
        reports = sorted(reports_dir.glob("*.md"), reverse=True)
        if reports:
            for report in reports[:10]:
                with st.expander(f"📄 {report.name}"):
                    st.markdown(report.read_text(encoding="utf-8")[:3000])
        else:
            render_empty_state("📄", "No quality reports generated yet.")
    else:
        render_empty_state("📂", "Reports directory not found.")


# ============================================================
# PAGE: Health Checks
# ============================================================
elif page == "Health Checks":
    st.markdown('<div class="page-title">System Health</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Run diagnostic checks on all pipeline components</div>', unsafe_allow_html=True
    )

    if st.button("🔍 Run Health Checks", use_container_width=False):
        with st.spinner("Running diagnostics..."):
            checker = HealthChecker(
                db_path=str(PROJECT_ROOT / "data" / "fittrack.duckdb"),
                data_dir=str(PROJECT_ROOT / "data"),
                log_dir=str(PROJECT_ROOT / "logs"),
            )
            results = checker.run_all_checks()

        healthy_count = sum(1 for r in results if r.healthy)
        total = len(results)

        # Summary banner
        if healthy_count == total:
            st.markdown(
                f"""
            <div class="glass-card" style="border-left: 4px solid #00D68F; text-align: center; padding: 1.5rem;">
                <span style="font-size: 2rem;">✅</span>
                <h3 style="color: #00D68F; margin: 0.5rem 0 0; font-weight: 600;">All {total} Health Checks Passed</h3>
            </div>
            """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
            <div class="glass-card" style="border-left: 4px solid #FFAA00; text-align: center; padding: 1.5rem;">
                <span style="font-size: 2rem;">⚠️</span>
                <h3 style="color: #FFAA00; margin: 0.5rem 0 0; font-weight: 600;">{healthy_count}/{total} Checks Passed</h3>
            </div>
            """,
                unsafe_allow_html=True,
            )

        # Individual check cards
        for r in results:
            card_class = "" if r.healthy else " unhealthy"
            icon = "✅" if r.healthy else "❌"
            st.markdown(
                f"""
            <div class="health-card{card_class}">
                <span style="font-size: 1.2rem; margin-right: 0.5rem;">{icon}</span>
                <span class="health-name">{r.name}</span>
                <div class="health-msg">{r.message}</div>
            </div>
            """,
                unsafe_allow_html=True,
            )
            if r.details:
                with st.expander(f"Details: {r.name}"):
                    st.json(r.details)
    else:
        st.markdown(
            """
        <div class="glass-card" style="text-align: center; padding: 3rem;">
            <div style="font-size: 3rem; margin-bottom: 0.75rem;">🏥</div>
            <h3 style="color: #FAFAFA; font-weight: 600;">System Health Monitor</h3>
            <p style="color: rgba(250,250,250,0.4);">Click the button above to run diagnostic checks on all pipeline components</p>
        </div>
        """,
            unsafe_allow_html=True,
        )


# ============================================================
# PAGE: Alerts
# ============================================================
elif page == "Alerts":
    st.markdown('<div class="page-title">Alerts</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-subtitle">Pipeline alerts and notifications</div>', unsafe_allow_html=True)

    alerts = load_alerts()

    if not alerts:
        st.markdown(
            """
        <div class="glass-card" style="text-align: center; border-left: 4px solid #00D68F; padding: 2rem;">
            <span style="font-size: 2rem;">✅</span>
            <h3 style="color: #00D68F; margin: 0.5rem 0 0; font-weight: 600;">No Alerts</h3>
            <p style="color: rgba(250,250,250,0.4);">System is running smoothly</p>
        </div>
        """,
            unsafe_allow_html=True,
        )
    else:
        # Alert counts
        critical = sum(1 for a in alerts if a.get("severity") == "CRITICAL")
        warning = sum(1 for a in alerts if a.get("severity") == "WARNING")
        info_count = sum(1 for a in alerts if a.get("severity") == "INFO")

        col1, col2, col3, col4 = st.columns([1, 1, 1, 1.5])
        with col1:
            render_metric_card("🔴", "Critical", critical, "danger")
        with col2:
            render_metric_card("🟡", "Warning", warning, "warning")
        with col3:
            render_metric_card("🔵", "Info", info_count, "info")
        with col4:
            st.plotly_chart(create_severity_donut(alerts), use_container_width=True, config=PLOTLY_CONFIG)

        # Severity filter
        render_section_header("Alert Log")
        severity_filter = st.multiselect(
            "Filter by severity",
            options=["CRITICAL", "WARNING", "INFO"],
            default=["CRITICAL", "WARNING", "INFO"],
            label_visibility="collapsed",
        )

        filtered = [a for a in alerts if a.get("severity") in severity_filter]

        for alert in reversed(filtered[-50:]):
            severity = alert.get("severity", "INFO")
            sev_class = {"CRITICAL": "critical", "WARNING": "warning", "INFO": "info"}.get(severity, "info")

            with st.expander(f"[{severity}] {alert.get('source', '?')}: {alert.get('message', '')[:80]}"):
                st.markdown(f"**Severity:** {render_status_badge(severity)}", unsafe_allow_html=True)
                st.markdown(f"**Time:** `{alert.get('timestamp', 'N/A')}`")
                st.markdown(f"**Source:** `{alert.get('source', 'N/A')}`")
                st.markdown(f"**Message:** {alert.get('message', 'N/A')}")
                if alert.get("details"):
                    st.json(alert["details"])
