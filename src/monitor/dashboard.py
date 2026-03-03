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

    /* ── Athlete profile card ── */
    .athlete-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 16px;
        padding: 1.5rem;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .athlete-card:hover {
        background: rgba(255, 255, 255, 0.06);
        transform: translateY(-3px);
        box-shadow: 0 12px 40px rgba(108, 99, 255, 0.15);
    }
    .athlete-name {
        font-size: 1.3rem;
        font-weight: 700;
        color: #FAFAFA;
        margin-bottom: 0.25rem;
    }
    .athlete-email {
        color: rgba(250, 250, 250, 0.4);
        font-size: 0.8rem;
        margin-bottom: 1rem;
    }
    .stat-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.75rem;
    }
    .stat-item .stat-label {
        color: rgba(250, 250, 250, 0.45);
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .stat-item .stat-value {
        color: #FAFAFA;
        font-size: 1.15rem;
        font-weight: 600;
    }
    .training-badge {
        display: inline-block;
        background: rgba(108, 99, 255, 0.15);
        color: #6C63FF;
        border-radius: 20px;
        padding: 0.2rem 0.75rem;
        font-size: 0.75rem;
        font-weight: 600;
        margin-bottom: 0.75rem;
    }

    /* ── Staggered animations ── */
    .main .block-container > div:nth-child(1) { animation-delay: 0s; }
    .main .block-container > div:nth-child(2) { animation-delay: 0.05s; }
    .main .block-container > div:nth-child(3) { animation-delay: 0.1s; }
    .main .block-container > div:nth-child(4) { animation-delay: 0.15s; }
    .main .block-container > div:nth-child(5) { animation-delay: 0.2s; }

    /* ── Filter card ── */
    .filter-bar {
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 1rem 1.25rem;
        margin-bottom: 1.25rem;
    }

    /* ── PR badge ── */
    .pr-badge {
        background: linear-gradient(135deg, #FFD700, #FFA500);
        color: #000;
        border-radius: 6px;
        padding: 0.15rem 0.5rem;
        font-size: 0.7rem;
        font-weight: 700;
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
# Quality trend chart factories
# ============================================================
def create_quality_trend_line(scores_df: pd.DataFrame) -> go.Figure:
    """Line chart showing overall_score over time, per table."""
    fig = go.Figure()
    colors = ["#6C63FF", "#00D68F", "#FFAA00", "#0095FF", "#FF3D71"]
    for i, table_name in enumerate(scores_df["table_name"].unique()):
        table_data = scores_df[scores_df["table_name"] == table_name].sort_values("scored_at")
        fig.add_trace(
            go.Scatter(
                x=table_data["scored_at"],
                y=table_data["overall_score"],
                mode="lines+markers",
                name=table_name.replace("_", " ").title(),
                marker=dict(size=8),
                line=dict(width=2, color=colors[i % len(colors)]),
                hovertemplate="<b>%{fullData.name}</b><br>Score: %{y:.1f}<br>Time: %{x}<extra></extra>",
            )
        )
    fig.update_layout(
        **get_plotly_defaults(),
        height=400,
        title=dict(text="Overall Quality Score Over Time", font=dict(size=16)),
        xaxis_title="",
        yaxis_title="Score",
    )
    fig.update_yaxes(range=[0, 105])
    return fig


def create_dimension_trend_lines(scores_df: pd.DataFrame, table_name: str) -> go.Figure:
    """Line chart showing 4 quality dimensions over time for a specific table."""
    table_data = scores_df[scores_df["table_name"] == table_name].sort_values("scored_at")
    dimensions = {
        "completeness_score": ("#00D68F", "Completeness"),
        "accuracy_score": ("#6C63FF", "Accuracy"),
        "consistency_score": ("#FFAA00", "Consistency"),
        "timeliness_score": ("#0095FF", "Timeliness"),
    }
    fig = go.Figure()
    for col, (color, label) in dimensions.items():
        if col in table_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=table_data["scored_at"],
                    y=table_data[col],
                    mode="lines+markers",
                    name=label,
                    line=dict(color=color, width=2),
                    marker=dict(size=6, color=color),
                    hovertemplate=f"<b>{label}</b><br>Score: %{{y:.1f}}<br>Time: %{{x}}<extra></extra>",
                )
            )
    fig.update_layout(
        **get_plotly_defaults(),
        height=350,
        title=dict(
            text=f"Dimension Trends: {table_name.replace('_', ' ').title()}",
            font=dict(size=15),
        ),
    )
    fig.update_yaxes(range=[0, 105])
    return fig


def create_quality_gate_bar(scores_df: pd.DataFrame) -> go.Figure:
    """Stacked bar chart showing quality gate pass/fail counts per table."""
    tables = sorted(scores_df["table_name"].unique())
    pass_counts = scores_df[scores_df["overall_score"] >= 50].groupby("table_name").size()
    fail_counts = scores_df[scores_df["overall_score"] < 50].groupby("table_name").size()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=[t.replace("_", " ").title() for t in tables],
            y=[int(pass_counts.get(t, 0)) for t in tables],
            name="Pass",
            marker_color="#00D68F",
            marker=dict(cornerradius=4),
            hovertemplate="<b>%{x}</b><br>Pass: %{y}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=[t.replace("_", " ").title() for t in tables],
            y=[int(fail_counts.get(t, 0)) for t in tables],
            name="Fail",
            marker_color="#FF3D71",
            marker=dict(cornerradius=4),
            hovertemplate="<b>%{x}</b><br>Fail: %{y}<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        barmode="stack",
        height=300,
        title=dict(text="Quality Gate Pass/Fail Rate", font=dict(size=16)),
        showlegend=True,
    )
    return fig


# ============================================================
# Analytics chart factories (Gold-layer data)
# ============================================================
def create_e1rm_chart(workouts_df: pd.DataFrame, exercise_name: str) -> go.Figure:
    """Estimated 1RM trend using Epley formula: weight * (1 + reps / 30)."""
    df = workouts_df[workouts_df["exercise"] == exercise_name].copy()
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No data for this exercise", showarrow=False, font=dict(size=14, color="rgba(250,250,250,0.3)")
        )
        fig.update_layout(**get_plotly_defaults(), height=350)
        return fig

    df["w"] = pd.to_numeric(df["weight"], errors="coerce")
    df["r"] = pd.to_numeric(df["reps"], errors="coerce")
    df["e1rm"] = (df["w"] * (1 + df["r"] / 30)).round(1)
    date_col = "workout_date" if "workout_date" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[date_col], errors="coerce")
    daily = df.groupby("dt")["e1rm"].max().reset_index().sort_values("dt")

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily["dt"],
            y=daily["e1rm"],
            mode="lines+markers",
            name="Est. 1RM",
            line=dict(color="#6C63FF", width=3),
            marker=dict(size=8, color="#6C63FF", line=dict(width=1, color="rgba(255,255,255,0.2)")),
            fill="tozeroy",
            fillcolor="rgba(108,99,255,0.08)",
            hovertemplate="<b>%{x|%b %d}</b><br>Est. 1RM: %{y:.1f} kg<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=350,
        title=dict(text=f"Estimated 1RM — {exercise_name}", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="kg",
    )
    return fig


def create_weekly_volume_chart(workouts_df: pd.DataFrame) -> go.Figure:
    """Weekly training volume bar chart."""
    df = workouts_df.copy()
    df["w"] = pd.to_numeric(df.get("weight", 0), errors="coerce").fillna(0)
    df["r"] = pd.to_numeric(df.get("reps", 0), errors="coerce").fillna(0)
    df["vol"] = df["w"] * df["r"]
    date_col = "workout_date" if "workout_date" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df["week"] = df["dt"].dt.isocalendar().week.astype(int)
    weekly = df.groupby("week")["vol"].sum().reset_index().sort_values("week")
    weekly["label"] = "W" + weekly["week"].astype(str)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=weekly["label"],
            y=weekly["vol"].round(0),
            marker=dict(color="#00D68F", cornerradius=6),
            hovertemplate="<b>%{x}</b><br>Volume: %{y:,.0f} kg<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=320,
        title=dict(text="Weekly Training Volume", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="Volume (kg)",
    )
    return fig


def create_muscle_volume_chart(workouts_df: pd.DataFrame, exercises_df: pd.DataFrame) -> go.Figure:
    """Horizontal bar — training volume by primary muscle group."""
    df = workouts_df.copy()
    df["w"] = pd.to_numeric(df.get("weight", 0), errors="coerce").fillna(0)
    df["r"] = pd.to_numeric(df.get("reps", 0), errors="coerce").fillna(0)
    df["vol"] = df["w"] * df["r"]

    muscle_map: dict[str, str] = {}
    if not exercises_df.empty and "name" in exercises_df.columns and "primary_muscle" in exercises_df.columns:
        muscle_map = dict(zip(exercises_df["name"], exercises_df["primary_muscle"], strict=False))
    df["muscle"] = df["exercise"].map(muscle_map).fillna("other")
    mv = df.groupby("muscle")["vol"].sum().sort_values(ascending=True)

    palette = {
        "chest": "#FF3D71",
        "back": "#0095FF",
        "shoulders": "#FFAA00",
        "quads": "#00D68F",
        "glutes": "#6C63FF",
        "hamstrings": "#FF6B6B",
        "biceps": "#4ECDC4",
        "triceps": "#45B7D1",
        "lats": "#96CEB4",
        "calves": "#FFEAA7",
        "abs": "#DDA0DD",
        "traps": "#98D8C8",
    }
    bar_colors = [palette.get(m, "#6C63FF") for m in mv.index]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=[m.replace("_", " ").title() for m in mv.index],
            x=mv.values.round(0),
            orientation="h",
            marker=dict(color=bar_colors, cornerradius=6),
            hovertemplate="<b>%{y}</b><br>Volume: %{x:,.0f} kg<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=400,
        title=dict(text="Volume by Muscle Group", font=dict(size=15)),
        xaxis_title="Total Volume (kg)",
        yaxis_title="",
    )
    return fig


def create_calorie_trend(nutrition_df: pd.DataFrame) -> go.Figure:
    """Daily calorie trend with 7-day moving average."""
    df = nutrition_df.copy()
    df["cal"] = pd.to_numeric(df.get("calories", 0), errors="coerce").fillna(0)
    df["dt"] = pd.to_datetime(df.get("log_date", ""), errors="coerce")
    daily = df.groupby("dt")["cal"].sum().reset_index().sort_values("dt")
    daily["ma7"] = daily["cal"].rolling(7, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=daily["dt"],
            y=daily["cal"].round(0),
            name="Daily",
            marker=dict(color="rgba(108,99,255,0.3)", cornerradius=4),
            hovertemplate="<b>%{x|%b %d}</b><br>%{y:,.0f} cal<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=daily["dt"],
            y=daily["ma7"].round(0),
            name="7-Day Avg",
            mode="lines",
            line=dict(color="#FFAA00", width=3),
            hovertemplate="<b>%{x|%b %d}</b><br>Avg: %{y:,.0f} cal<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=350,
        barmode="overlay",
        title=dict(text="Daily Calorie Intake", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="Calories",
    )
    return fig


def create_macro_donut(nutrition_df: pd.DataFrame) -> go.Figure:
    """Macro split donut chart."""
    p = (
        float(pd.to_numeric(nutrition_df["protein_g"], errors="coerce").sum())
        if "protein_g" in nutrition_df.columns
        else 0.0
    )
    c = (
        float(pd.to_numeric(nutrition_df["carbs_g"], errors="coerce").sum())
        if "carbs_g" in nutrition_df.columns
        else 0.0
    )
    f = float(pd.to_numeric(nutrition_df["fats_g"], errors="coerce").sum()) if "fats_g" in nutrition_df.columns else 0.0
    fig = go.Figure(
        go.Pie(
            labels=["Protein", "Carbs", "Fats"],
            values=[p, c, f],
            hole=0.6,
            marker=dict(colors=["#00D68F", "#0095FF", "#FFAA00"]),
            textinfo="percent+label",
            textfont=dict(size=12, color="#FAFAFA"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f}g (%{percent})<extra></extra>",
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color="#FAFAFA"),
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        showlegend=False,
        title=dict(text="Macro Split", font=dict(size=15)),
    )
    return fig


def create_meal_distribution(nutrition_df: pd.DataFrame) -> go.Figure:
    """Average calories by meal type."""
    df = nutrition_df.copy()
    df["cal"] = pd.to_numeric(df.get("calories", 0), errors="coerce").fillna(0)
    if "meal_type" not in df.columns:
        fig = go.Figure()
        fig.update_layout(**get_plotly_defaults(), height=300)
        return fig
    avg = df.groupby("meal_type")["cal"].mean().sort_values(ascending=False)
    mc = {
        "breakfast": "#FFAA00",
        "lunch": "#0095FF",
        "dinner": "#6C63FF",
        "snack": "#00D68F",
        "pre_workout": "#FF6B6B",
        "post_workout": "#4ECDC4",
        "supplement": "#96CEB4",
    }
    fig = go.Figure(
        go.Bar(
            x=[m.replace("_", " ").title() for m in avg.index],
            y=avg.values.round(0),
            marker=dict(color=[mc.get(m, "#6C63FF") for m in avg.index], cornerradius=6),
            hovertemplate="<b>%{x}</b><br>Avg: %{y:,.0f} cal<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=320,
        title=dict(text="Avg Calories by Meal Type", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="Calories",
    )
    return fig


def create_weight_trend(metrics_df: pd.DataFrame) -> go.Figure:
    """Weight trend with 7-day moving average."""
    df = metrics_df.copy()
    df["w"] = pd.to_numeric(df.get("weight_kg", 0), errors="coerce")
    date_col = "measured_at" if "measured_at" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values("dt")
    df["ma7"] = df["w"].rolling(7, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["dt"],
            y=df["w"],
            mode="markers",
            name="Daily",
            marker=dict(size=6, color="rgba(108,99,255,0.5)"),
            hovertemplate="<b>%{x|%b %d}</b><br>%{y:.1f} kg<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["dt"],
            y=df["ma7"].round(1),
            mode="lines",
            name="7-Day Avg",
            line=dict(color="#6C63FF", width=3),
            hovertemplate="<b>%{x|%b %d}</b><br>Avg: %{y:.1f} kg<extra></extra>",
        )
    )
    fig.update_layout(
        **get_plotly_defaults(),
        height=350,
        title=dict(text="Weight Trend", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="kg",
    )
    return fig


def create_body_comp_chart(metrics_df: pd.DataFrame) -> go.Figure:
    """Lean mass vs fat mass over time."""
    df = metrics_df.copy()
    date_col = "measured_at" if "measured_at" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values("dt")
    fig = go.Figure()
    if "lean_mass_calc_kg" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["dt"],
                y=pd.to_numeric(df["lean_mass_calc_kg"], errors="coerce"),
                mode="lines+markers",
                name="Lean Mass",
                line=dict(color="#00D68F", width=2),
                marker=dict(size=5),
                hovertemplate="<b>%{x|%b %d}</b><br>Lean: %{y:.1f} kg<extra></extra>",
            )
        )
    if "fat_mass_kg" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["dt"],
                y=pd.to_numeric(df["fat_mass_kg"], errors="coerce"),
                mode="lines+markers",
                name="Fat Mass",
                line=dict(color="#FF3D71", width=2),
                marker=dict(size=5),
                hovertemplate="<b>%{x|%b %d}</b><br>Fat: %{y:.1f} kg<extra></extra>",
            )
        )
    fig.update_layout(
        **get_plotly_defaults(),
        height=350,
        title=dict(text="Body Composition", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="kg",
    )
    return fig


def create_recovery_chart(metrics_df: pd.DataFrame) -> go.Figure:
    """Recovery score and sleep quality trends."""
    df = metrics_df.copy()
    date_col = "measured_at" if "measured_at" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.sort_values("dt")
    fig = go.Figure()
    if "recovery_score" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["dt"],
                y=pd.to_numeric(df["recovery_score"], errors="coerce"),
                mode="lines",
                name="Recovery",
                line=dict(color="#00D68F", width=2),
                hovertemplate="<b>%{x|%b %d}</b><br>Recovery: %{y}<extra></extra>",
            )
        )
    if "sleep_quality" in df.columns:
        sleep = pd.to_numeric(df["sleep_quality"], errors="coerce")
        fig.add_trace(
            go.Scatter(
                x=df["dt"],
                y=sleep * 10,
                mode="lines",
                name="Sleep (x10)",
                line=dict(color="#0095FF", width=2, dash="dot"),
                customdata=sleep,
                hovertemplate="<b>%{x|%b %d}</b><br>Sleep: %{customdata}/10<extra></extra>",
            )
        )
    fig.update_layout(
        **get_plotly_defaults(),
        height=320,
        title=dict(text="Recovery & Sleep Trends", font=dict(size=15)),
        xaxis_title="",
        yaxis_title="Score",
    )
    fig.update_yaxes(range=[0, 105])
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


def load_gold_table(table_name: str) -> pd.DataFrame:
    """Load a Gold-layer table from DuckDB."""
    db_path = PROJECT_ROOT / "data" / "fittrack.duckdb"
    if not db_path.exists():
        return pd.DataFrame()
    try:
        import duckdb

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        except duckdb.CatalogException:
            df = pd.DataFrame()
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


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
        [
            "Overview",
            "Athlete Profiles",
            "Strength Analytics",
            "Nutrition Analytics",
            "Body Composition",
            "Pipeline Runs",
            "Data Quality",
            "Quality Trends",
            "Health Checks",
            "Alerts",
        ],
        index=0,
        label_visibility="collapsed",
    )

    st.divider()

    st.caption(f"v2.0.0 · {datetime.now().strftime('%H:%M:%S')}")
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
                    <strong>{alert.get("source", "?")}</strong>: {alert.get("message", "")}
                </span>
                <span style="float: right; color: rgba(250,250,250,0.35); font-size: 0.8rem;">
                    {alert.get("timestamp", "")[:19]}
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
# PAGE: Athlete Profiles
# ============================================================
elif page == "Athlete Profiles":
    st.markdown('<div class="page-title">Athlete Profiles</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Individual athlete summaries and key performance indicators</div>',
        unsafe_allow_html=True,
    )

    metrics_df = load_gold_table("gold_body_metrics")
    workouts_df = load_gold_table("gold_workouts")
    nutrition_df = load_gold_table("gold_nutrition_logs")
    athletes_df = load_gold_table("dim_athletes")

    if metrics_df.empty and workouts_df.empty:
        render_empty_state("👥", "No athlete data yet. Run the pipeline to populate profiles.")
    else:
        athlete_ids: set[str] = set()
        for check_df in [metrics_df, workouts_df, nutrition_df]:
            if "athlete_id" in check_df.columns:
                athlete_ids.update(check_df["athlete_id"].unique())

        name_map: dict[str, str] = {}
        style_map: dict[str, str] = {}
        if not athletes_df.empty and "email" in athletes_df.columns:
            name_map = dict(zip(athletes_df["email"], athletes_df.get("full_name", athletes_df["email"]), strict=False))
            if "activity_level" in athletes_df.columns:
                style_map = dict(zip(athletes_df["email"], athletes_df["activity_level"], strict=False))

        athletes_sorted = sorted(athlete_ids)
        accent_colors = ["#6C63FF", "#00D68F", "#FFAA00", "#0095FF", "#FF3D71", "#4ECDC4"]

        for idx in range(0, len(athletes_sorted), 3):
            cols = st.columns(3)
            for j, col in enumerate(cols):
                if idx + j >= len(athletes_sorted):
                    break
                aid = athletes_sorted[idx + j]
                name = name_map.get(aid, aid.split("@")[0].replace(".", " ").title())
                style = style_map.get(aid, "athlete").replace("_", " ").title()
                accent = accent_colors[(idx + j) % len(accent_colors)]

                a_m = (
                    metrics_df[metrics_df["athlete_id"] == aid]
                    if not metrics_df.empty and "athlete_id" in metrics_df.columns
                    else pd.DataFrame()
                )
                a_w = (
                    workouts_df[workouts_df["athlete_id"] == aid]
                    if not workouts_df.empty and "athlete_id" in workouts_df.columns
                    else pd.DataFrame()
                )
                a_n = (
                    nutrition_df[nutrition_df["athlete_id"] == aid]
                    if not nutrition_df.empty and "athlete_id" in nutrition_df.columns
                    else pd.DataFrame()
                )

                avg_wt = (
                    pd.to_numeric(a_m.get("weight_kg", pd.Series(dtype=float)), errors="coerce").mean()
                    if not a_m.empty
                    else 0
                )
                avg_bf = (
                    pd.to_numeric(a_m.get("body_fat_pct", pd.Series(dtype=float)), errors="coerce").mean()
                    if not a_m.empty
                    else 0
                )
                t_days = a_w["workout_date"].nunique() if not a_w.empty and "workout_date" in a_w.columns else 0
                total_sets = len(a_w)
                daily_cal = (
                    pd.to_numeric(a_n["calories"], errors="coerce").groupby(a_n["log_date"]).sum().mean()
                    if not a_n.empty and "log_date" in a_n.columns
                    else 0
                )

                with col:
                    st.markdown(
                        f"""
                    <div class="athlete-card" style="border-left: 4px solid {accent};">
                        <div class="athlete-name">{name}</div>
                        <div class="athlete-email">{aid}</div>
                        <div class="training-badge">{style}</div>
                        <div class="stat-grid">
                            <div class="stat-item">
                                <div class="stat-label">Avg Weight</div>
                                <div class="stat-value">{avg_wt:.1f} kg</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Body Fat</div>
                                <div class="stat-value">{avg_bf:.1f}%</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Training Days</div>
                                <div class="stat-value">{t_days}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Daily Calories</div>
                                <div class="stat-value">{daily_cal:,.0f}</div>
                            </div>
                        </div>
                        <div style="margin-top: 0.75rem; color: rgba(250,250,250,0.35); font-size: 0.8rem;">
                            {total_sets} total sets logged
                        </div>
                    </div>
                    """,
                        unsafe_allow_html=True,
                    )


# ============================================================
# PAGE: Strength Analytics
# ============================================================
elif page == "Strength Analytics":
    st.markdown('<div class="page-title">Strength Analytics</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Progressive overload tracking and training volume analysis</div>',
        unsafe_allow_html=True,
    )

    workouts_df = load_gold_table("gold_workouts")
    exercises_df = load_gold_table("gold_exercises")

    if workouts_df.empty:
        render_empty_state("💪", "No workout data yet. Run the pipeline to see strength analytics.")
    else:
        # Filters
        st.markdown('<div class="filter-bar">', unsafe_allow_html=True)
        f1, f2 = st.columns(2)
        with f1:
            athletes_in_data = sorted(workouts_df["athlete_id"].unique()) if "athlete_id" in workouts_df.columns else []
            athlete_filter = st.selectbox("Athlete", ["All Athletes", *athletes_in_data], key="str_ath")
        with f2:
            exercises_in_data = sorted(workouts_df["exercise"].unique()) if "exercise" in workouts_df.columns else []
            exercise_filter = st.selectbox("Exercise (for 1RM chart)", exercises_in_data, key="str_ex")
        st.markdown("</div>", unsafe_allow_html=True)

        filtered = workouts_df.copy()
        if athlete_filter != "All Athletes" and "athlete_id" in filtered.columns:
            filtered = filtered[filtered["athlete_id"] == athlete_filter]

        # e1RM chart
        render_section_header("Estimated 1RM Progression")
        if exercise_filter:
            st.plotly_chart(
                create_e1rm_chart(filtered, exercise_filter), use_container_width=True, config=PLOTLY_CONFIG
            )

        # Volume charts
        c1, c2 = st.columns(2)
        with c1:
            render_section_header("Weekly Volume")
            st.plotly_chart(create_weekly_volume_chart(filtered), use_container_width=True, config=PLOTLY_CONFIG)
        with c2:
            render_section_header("Volume by Muscle Group")
            st.plotly_chart(
                create_muscle_volume_chart(filtered, exercises_df), use_container_width=True, config=PLOTLY_CONFIG
            )

        # PR Table
        render_section_header("Personal Records (Top Lifts)")
        pr_df = filtered.copy()
        pr_df["w"] = pd.to_numeric(pr_df.get("weight", 0), errors="coerce").fillna(0)
        pr_df["r"] = pd.to_numeric(pr_df.get("reps", 0), errors="coerce").fillna(0)
        pr_df["e1rm"] = (pr_df["w"] * (1 + pr_df["r"] / 30)).round(1)
        pr_table = (
            pr_df.groupby("exercise")
            .agg(
                best_e1rm=("e1rm", "max"),
                heaviest=("w", "max"),
                max_reps=("r", "max"),
                total_sets=("exercise", "count"),
            )
            .sort_values("best_e1rm", ascending=False)
            .reset_index()
        )
        pr_table.columns = ["Exercise", "Est. 1RM (kg)", "Heaviest (kg)", "Max Reps", "Total Sets"]
        st.dataframe(pr_table, use_container_width=True, hide_index=True)


# ============================================================
# PAGE: Nutrition Analytics
# ============================================================
elif page == "Nutrition Analytics":
    st.markdown('<div class="page-title">Nutrition Analytics</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Macro tracking, calorie trends, and meal-level insights</div>',
        unsafe_allow_html=True,
    )

    nutrition_df = load_gold_table("gold_nutrition_logs")
    metrics_df = load_gold_table("gold_body_metrics")

    if nutrition_df.empty:
        render_empty_state("🥗", "No nutrition data yet. Run the pipeline to see nutrition analytics.")
    else:
        # Filter
        st.markdown('<div class="filter-bar">', unsafe_allow_html=True)
        athletes_in_data = sorted(nutrition_df["athlete_id"].unique()) if "athlete_id" in nutrition_df.columns else []
        athlete_filter = st.selectbox("Athlete", ["All Athletes", *athletes_in_data], key="nut_ath")
        st.markdown("</div>", unsafe_allow_html=True)

        filtered = nutrition_df.copy()
        filtered_metrics = metrics_df.copy()
        if athlete_filter != "All Athletes":
            if "athlete_id" in filtered.columns:
                filtered = filtered[filtered["athlete_id"] == athlete_filter]
            if "athlete_id" in filtered_metrics.columns:
                filtered_metrics = filtered_metrics[filtered_metrics["athlete_id"] == athlete_filter]

        # Top KPIs
        total_cals = pd.to_numeric(filtered.get("calories", 0), errors="coerce")
        total_protein = pd.to_numeric(filtered.get("protein_g", 0), errors="coerce")
        n_days = filtered["log_date"].nunique() if "log_date" in filtered.columns else 1
        avg_daily_cal = total_cals.sum() / max(n_days, 1)
        avg_daily_protein = total_protein.sum() / max(n_days, 1)
        avg_weight = (
            pd.to_numeric(filtered_metrics.get("weight_kg", pd.Series(dtype=float)), errors="coerce").mean()
            if not filtered_metrics.empty
            else 0
        )
        protein_per_kg = avg_daily_protein / avg_weight if avg_weight > 0 else 0

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            render_metric_card("🔥", "Avg Daily Cal", f"{avg_daily_cal:,.0f}")
        with k2:
            render_metric_card("🥩", "Avg Daily Protein", f"{avg_daily_protein:.0f}g")
        with k3:
            render_metric_card("⚖️", "Protein/kg", f"{protein_per_kg:.1f}g")
        with k4:
            render_metric_card("📅", "Days Tracked", n_days)

        # Charts
        c1, c2 = st.columns([2, 1])
        with c1:
            render_section_header("Calorie Trend")
            st.plotly_chart(create_calorie_trend(filtered), use_container_width=True, config=PLOTLY_CONFIG)
        with c2:
            render_section_header("Macro Split")
            st.plotly_chart(create_macro_donut(filtered), use_container_width=True, config=PLOTLY_CONFIG)

        render_section_header("Calories by Meal Type")
        st.plotly_chart(create_meal_distribution(filtered), use_container_width=True, config=PLOTLY_CONFIG)


# ============================================================
# PAGE: Body Composition
# ============================================================
elif page == "Body Composition":
    st.markdown('<div class="page-title">Body Composition</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Weight trends, body fat analysis, and recovery tracking</div>',
        unsafe_allow_html=True,
    )

    metrics_df = load_gold_table("gold_body_metrics")

    if metrics_df.empty:
        render_empty_state("📊", "No body metrics yet. Run the pipeline to see composition data.")
    else:
        # Filter
        st.markdown('<div class="filter-bar">', unsafe_allow_html=True)
        athletes_in_data = sorted(metrics_df["athlete_id"].unique()) if "athlete_id" in metrics_df.columns else []
        athlete_filter = st.selectbox("Athlete", ["All Athletes", *athletes_in_data], key="comp_ath")
        st.markdown("</div>", unsafe_allow_html=True)

        filtered = metrics_df.copy()
        if athlete_filter != "All Athletes" and "athlete_id" in filtered.columns:
            filtered = filtered[filtered["athlete_id"] == athlete_filter]

        # Top KPIs
        wt = pd.to_numeric(filtered.get("weight_kg", pd.Series(dtype=float)), errors="coerce")
        bf = pd.to_numeric(filtered.get("body_fat_pct", pd.Series(dtype=float)), errors="coerce")
        rhr = pd.to_numeric(filtered.get("resting_heart_rate", pd.Series(dtype=float)), errors="coerce")
        steps = pd.to_numeric(filtered.get("steps", pd.Series(dtype=float)), errors="coerce")

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            render_metric_card("⚖️", "Latest Weight", f"{wt.iloc[-1]:.1f} kg" if len(wt) > 0 else "—")
        with k2:
            render_metric_card("📏", "Avg Body Fat", f"{bf.mean():.1f}%" if len(bf) > 0 else "—")
        with k3:
            render_metric_card("❤️", "Avg RHR", f"{rhr.mean():.0f} bpm" if len(rhr) > 0 else "—")
        with k4:
            render_metric_card("👟", "Avg Steps", f"{steps.mean():,.0f}" if len(steps) > 0 else "—")

        # Charts row 1
        c1, c2 = st.columns(2)
        with c1:
            render_section_header("Weight Trend")
            st.plotly_chart(create_weight_trend(filtered), use_container_width=True, config=PLOTLY_CONFIG)
        with c2:
            render_section_header("Body Composition")
            st.plotly_chart(create_body_comp_chart(filtered), use_container_width=True, config=PLOTLY_CONFIG)

        # Charts row 2
        render_section_header("Recovery & Sleep")
        st.plotly_chart(create_recovery_chart(filtered), use_container_width=True, config=PLOTLY_CONFIG)


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
                            f"{table.replace('_', ' ').title()}</div>",
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
# PAGE: Quality Trends
# ============================================================
elif page == "Quality Trends":
    st.markdown('<div class="page-title">Quality Trends</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Track quality score evolution over time</div>',
        unsafe_allow_html=True,
    )

    scores_df = load_quality_scores()

    if scores_df.empty or "scored_at" not in scores_df.columns:
        render_empty_state("📈", "No quality trend data yet. Run the pipeline multiple times to see trends.")
    else:
        scores_df["scored_at"] = pd.to_datetime(scores_df["scored_at"])
        tables = sorted(scores_df["table_name"].unique())

        # Delta indicators (latest vs previous)
        render_section_header("Score Changes (Latest vs Previous)")
        delta_cols = st.columns(len(tables))
        for i, table in enumerate(tables):
            table_scores = scores_df[scores_df["table_name"] == table].sort_values("scored_at", ascending=False)
            latest = float(table_scores.iloc[0]["overall_score"]) if len(table_scores) >= 1 else 0.0
            previous = float(table_scores.iloc[1]["overall_score"]) if len(table_scores) >= 2 else latest
            delta = latest - previous
            delta_str = f"+{delta:.1f}" if delta >= 0 else f"{delta:.1f}"
            color = "#00D68F" if delta >= 0 else "#FF3D71"
            with delta_cols[i]:
                st.markdown(
                    f"""
                <div class="metric-card">
                    <div class="metric-label">{table.replace("_", " ").title()}</div>
                    <div class="metric-value">{latest:.1f}</div>
                    <div style="color: {color}; font-weight: 600; font-size: 0.95rem;">{delta_str}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

        # Overall trend line chart
        render_section_header("Overall Score Trends")
        st.plotly_chart(create_quality_trend_line(scores_df), use_container_width=True, config=PLOTLY_CONFIG)

        # Quality gate pass/fail bar chart
        render_section_header("Quality Gate Results")
        st.plotly_chart(create_quality_gate_bar(scores_df), use_container_width=True, config=PLOTLY_CONFIG)

        # Per-table dimension trends
        render_section_header("Dimension Trends by Table")
        selected_table = st.selectbox("Select table", tables)
        if selected_table:
            st.plotly_chart(
                create_dimension_trend_lines(scores_df, selected_table),
                use_container_width=True,
                config=PLOTLY_CONFIG,
            )


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
