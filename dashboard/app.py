# /Users/bikal/Data_scraping/dashboard/app.py

import os
import traceback

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, callback


# =========================
# CONFIG (EDIT ONLY THIS)
# =========================
# Local Mac path (works locally). For Plotly Cloud, replace with a relative repo path.
DATA_DIR = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"
MASTER_FILE = os.path.join(DATA_DIR, "jobs_master.xlsx")

REFRESH_SECONDS = 30  # your style from grep


# =========================
# HELPERS
# =========================
PLACEHOLDERS = ["Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE"]


def _empty_fig(title: str) -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=title,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{
            "text": "No data available",
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "x": 0.5,
            "y": 0.5,
            "font": {"size": 14}
        }],
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def _load_master() -> pd.DataFrame:
    if not os.path.exists(MASTER_FILE):
        raise FileNotFoundError(f"Master file not found: {MASTER_FILE}")

    df = pd.read_excel(MASTER_FILE, engine="openpyxl")
    df = df.replace(PLACEHOLDERS, pd.NA)

    # Ensure columns exist (so charts don't crash)
    required_cols = [
        "source",
        "global_key",
        "title",
        "company",
        "location",
        "employment_type",
        "category_primary",
        "scraped_at",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Backward compatibility: map category_primary -> it_non_it
    if "it_non_it" not in df.columns and "category_primary" in df.columns:
        df["it_non_it"] = df["category_primary"]

    # Normalize source
    df["source"] = df["source"].astype(str).str.strip().str.lower()
    df.loc[df["source"].isin(["<na>", "nan", "none"]), "source"] = pd.NA

    # Parse datetime safely
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    return df


# =========================
# FIGURE BUILDERS
# =========================
def fig_jobs_per_portal(df: pd.DataFrame) -> go.Figure:
    if df.empty or df["source"].isna().all():
        return _empty_fig("Jobs per portal")

    if df["global_key"].notna().any():
        agg = df.groupby("source")["global_key"].nunique().reset_index(name="jobs")
    else:
        agg = df.groupby("source").size().reset_index(name="jobs")

    if agg.empty:
        return _empty_fig("Jobs per portal")

    fig = px.bar(agg, x="source", y="jobs", title="Jobs per portal")
    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig


def fig_it_nonit(df: pd.DataFrame) -> go.Figure:
    if df.empty or "it_non_it" not in df.columns:
        return _empty_fig("IT vs Non-IT by portal")

    temp = df.copy()
    temp["it_non_it"] = temp["it_non_it"].fillna("Unknown").astype(str).str.strip()

    if temp["source"].isna().all():
        return _empty_fig("IT vs Non-IT by portal")

    agg = temp.groupby(["source", "it_non_it"]).size().reset_index(name="count")

    if agg.empty:
        return _empty_fig("IT vs Non-IT by portal")

    fig = px.bar(
        agg,
        x="source",
        y="count",
        color="it_non_it",
        barmode="group",
        title="IT vs Non-IT by portal",
    )
    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig


def fig_top_locations(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    if df.empty or df["location"].isna().all():
        return _empty_fig("Top locations")

    temp = df.copy()
    temp["location"] = temp["location"].fillna("Unknown").astype(str).str.strip()

    agg = temp["location"].value_counts().head(top_n).reset_index()
    agg.columns = ["location", "count"]

    if agg.empty:
        return _empty_fig("Top locations")

    fig = px.bar(
        agg,
        x="count",
        y="location",
        orientation="h",
        title=f"Top {top_n} locations",
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def fig_employment_type(df: pd.DataFrame) -> go.Figure:
    if df.empty or df["employment_type"].isna().all():
        return _empty_fig("Employment type distribution")

    temp = df.copy()
    temp["employment_type"] = temp["employment_type"].fillna("Unknown").astype(str).str.strip()

    agg = temp["employment_type"].value_counts().reset_index()
    agg.columns = ["employment_type", "count"]

    if agg.empty:
        return _empty_fig("Employment type distribution")

    fig = px.bar(agg, x="employment_type", y="count", title="Employment type distribution")
    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig


def fig_scrape_trend(df: pd.DataFrame) -> go.Figure:
    # Needs scraped_at datetime
    if df.empty or df["scraped_at"].isna().all():
        return _empty_fig("Scrape trend (hourly)")

    trend = df.dropna(subset=["scraped_at"]).copy()
    if trend.empty:
        return _empty_fig("Scrape trend (hourly)")

    # ✅ FIX: use lowercase 'h' (not 'H')
    trend["bucket"] = trend["scraped_at"].dt.floor("h")

    agg = trend.groupby("bucket").size().reset_index(name="count").sort_values("bucket")

    if agg.empty:
        return _empty_fig("Scrape trend (hourly)")

    fig = px.line(agg, x="bucket", y="count", markers=True, title="Scrape trend (hourly)")
    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig


# =========================
# DASH APP (USING YOUR IDs)
# =========================
app = Dash(__name__)

app.layout = html.Div(
    style={"padding": "14px", "fontFamily": "Arial"},
    children=[
        html.H2("Nepal Job Market Dashboard"),

        # ✅ id="status"
        html.Div(id="status", style={"whiteSpace": "pre-wrap", "marginBottom": "10px"}),

        # ✅ id="interval"
        dcc.Interval(id="interval", interval=REFRESH_SECONDS * 1000, n_intervals=0),

        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "12px"},
            children=[
                dcc.Graph(id="jobs_per_portal"),
                dcc.Graph(id="it_nonit_by_portal"),
                dcc.Graph(id="top_locations"),
                dcc.Graph(id="employment_type"),
                dcc.Graph(id="scrape_trend"),
            ],
        ),
    ],
)


# =========================
# CALLBACK (MATCHES YOUR IDs)
# =========================
@callback(
    Output("status", "children"),
    Output("jobs_per_portal", "figure"),
    Output("it_nonit_by_portal", "figure"),
    Output("top_locations", "figure"),
    Output("employment_type", "figure"),
    Output("scrape_trend", "figure"),
    Input("interval", "n_intervals"),
)
def update_dashboard(n_intervals):
    # Always return 6 outputs.
    try:
        df = _load_master()

        f1 = fig_jobs_per_portal(df)
        f2 = fig_it_nonit(df)
        f3 = fig_top_locations(df, top_n=10)
        f4 = fig_employment_type(df)
        f5 = fig_scrape_trend(df)

        portals = sorted([p for p in df["source"].dropna().unique().tolist()])
        msg = (
            f"✅ Updated successfully\n"
            f"File: {MASTER_FILE}\n"
            f"Rows: {len(df)}\n"
            f"Portals: {', '.join(portals) if portals else 'None'}\n"
            f"Tick: {n_intervals}\n"
            f"Refresh: {REFRESH_SECONDS}s"
        )

        return msg, f1, f2, f3, f4, f5

    except Exception:
        err = traceback.format_exc()
        return (
            f"❌ Callback failed:\n{err}",
            _empty_fig("Jobs per portal"),
            _empty_fig("IT vs Non-IT by portal"),
            _empty_fig("Top locations"),
            _empty_fig("Employment type distribution"),
            _empty_fig("Scrape trend (hourly)"),
        )


# =========================
# RUN LOCAL
# =========================
if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8050)
