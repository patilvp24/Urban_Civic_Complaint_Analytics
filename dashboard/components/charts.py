"""
Reusable Plotly Dark Theme Visualizations for Civic Complaint Analytics
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

DARK_THEME_LAYOUT = {
    "paper_bgcolor": "rgba(18, 22, 34, 0.8)",
    "plot_bgcolor": "rgba(18, 22, 34, 0.8)",
    "font": {"color": "#e0e6ed", "family": "Inter, sans-serif"},
    "xaxis": {
        "gridcolor": "#2c3245",
        "showline": True,
        "linecolor": "#3d4663",
        "zeroline": False
    },
    "yaxis": {
        "gridcolor": "#2c3245",
        "showline": True,
        "linecolor": "#3d4663",
        "zeroline": False
    },
    "margin": {"l": 40, "r": 30, "t": 45, "b": 40}
}

PALETTE = ["#00d2ff", "#00e676", "#ff5252", "#ffd740", "#7c4dff", "#ff9100", "#e040fb", "#18ffff"]


def create_category_bar(df: pd.DataFrame, top_n: int = 15) -> go.Figure:
    sub_df = df.head(top_n).sort_values("count", ascending=True)
    fig = go.Figure(go.Bar(
        x=sub_df["count"],
        y=sub_df["complaint_type"],
        orientation="h",
        marker=dict(
            color=sub_df["count"],
            colorscale="Viridis",
            showscale=False
        ),
        hovertemplate="<b>%{y}</b><br>Requests: %{x:,}<br>Share: %{customdata}%<extra></extra>",
        customdata=sub_df.get("percentage", [0] * len(sub_df))
    ))
    fig.update_layout(
        title=f"Top {top_n} Complaint Categories by Volume",
        xaxis_title="Number of Requests",
        yaxis_title="Complaint Category",
        height=max(450, top_n * 26),
        **DARK_THEME_LAYOUT
    )
    return fig


def create_agency_bar(df: pd.DataFrame, top_n: int = 12) -> go.Figure:
    sub_df = df.head(top_n)
    fig = go.Figure(go.Bar(
        x=sub_df["agency"],
        y=sub_df["count"],
        marker_color="#00d2ff",
        hovertemplate="<b>Agency: %{x}</b><br>Total Requests: %{y:,}<br>Share: %{customdata}%<extra></extra>",
        customdata=sub_df.get("percentage", [0] * len(sub_df))
    ))
    fig.update_layout(
        title=f"Top {top_n} Responding Agencies by Workload",
        xaxis_title="City Agency Code",
        yaxis_title="Total Requests Handled",
        height=420,
        **DARK_THEME_LAYOUT
    )
    return fig


def create_status_donut(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Pie(
        labels=df["status"],
        values=df["count"],
        hole=0.55,
        marker=dict(colors=PALETTE[:len(df)]),
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>Proportion: %{percent}<extra></extra>"
    ))
    fig.update_layout(
        title="Civic Service Request Status Distribution",
        height=400,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
        **DARK_THEME_LAYOUT
    )
    return fig


def create_monthly_trend_line(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["year_month"],
        y=df["count"],
        mode="lines+markers",
        line=dict(color="#00e676", width=3),
        marker=dict(size=7, color="#00e676", symbol="circle"),
        fill="tozeroy",
        fillcolor="rgba(0, 230, 118, 0.12)",
        hovertemplate="<b>Period: %{x}</b><br>Requests: %{y:,}<extra></extra>"
    ))
    fig.update_layout(
        title="Monthly Complaint Volume Trend (2020 - Present)",
        xaxis_title="Year-Month",
        yaxis_title="Monthly Requests",
        height=420,
        **DARK_THEME_LAYOUT
    )
    return fig


def create_borough_bar(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=df["borough"],
        y=df["count"],
        marker_color=["#7c4dff", "#00d2ff", "#00e676", "#ffd740", "#ff9100", "#90a4ae"][:len(df)],
        hovertemplate="<b>%{x}</b><br>Complaints: %{y:,}<br>Share: %{customdata}%<extra></extra>",
        customdata=df.get("percentage", [0] * len(df))
    ))
    fig.update_layout(
        title="Civic Complaints Distribution Across NYC Boroughs",
        xaxis_title="Borough",
        yaxis_title="Total Complaints",
        height=420,
        **DARK_THEME_LAYOUT
    )
    return fig


def create_resolution_bar(df: pd.DataFrame, top_n: int = 12, metric: str = "avg_days") -> go.Figure:
    sub_df = df.head(top_n).sort_values(metric, ascending=True)
    entity_col = "agency" if "agency" in sub_df.columns else "complaint_type"
    unit = "Days" if metric == "avg_days" else "Hours"
    
    fig = go.Figure(go.Bar(
        x=sub_df[metric],
        y=sub_df[entity_col],
        orientation="h",
        marker=dict(
            color=sub_df[metric],
            colorscale="Plasma",
            showscale=False
        ),
        hovertemplate=f"<b>%{{y}}</b><br>Avg Resolution: %{{x:.2f}} {unit}<br>Resolved Count: %{{customdata:,}}<extra></extra>",
        customdata=sub_df["resolved_count"]
    ))
    fig.update_layout(
        title=f"Average Resolution Time ({unit}) - Top {top_n} by Volume",
        xaxis_title=f"Average Duration ({unit})",
        yaxis_title="Agency / Category",
        height=max(420, top_n * 28),
        **DARK_THEME_LAYOUT
    )
    return fig


def create_agency_status_stacked(df: pd.DataFrame, top_agencies: int = 8) -> go.Figure:
    # Filter to top agencies
    top_agency_list = df.groupby("agency")["count"].sum().nlargest(top_agencies).index
    filtered = df[df["agency"].isin(top_agency_list)]
    
    pivot = filtered.pivot(index="agency", columns="status", values="count").fillna(0)
    
    fig = go.Figure()
    for col in pivot.columns:
        fig.add_trace(go.Bar(
            name=col,
            x=pivot.index,
            y=pivot[col],
            hovertemplate=f"<b>%{{x}}</b><br>Status: {col}<br>Count: %{{y:,}}<extra></extra>"
        ))
    fig.update_layout(
        barmode="stack",
        title=f"Status Composition Across Top {top_agencies} Agencies",
        xaxis_title="Agency",
        yaxis_title="Request Count",
        height=440,
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
        **DARK_THEME_LAYOUT
    )
    return fig
