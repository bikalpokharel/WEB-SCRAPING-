# dashboard/highlights.py
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, callback


# =========================
# CONFIG
# =========================
LOCAL_MASTER_CSV = "/Users/bikal/Data_scraping/data_local/jobs_master_local.csv"
REFRESH_SECONDS = 15  # checks file mtime + refreshes graphs

COL_TIME = "scraped_at"
COL_TITLE = "title"
COL_COMPANY = "company"
COL_LOCATION = "location"
COL_COUNTRY = "country"
COL_SOURCE = "source"
COL_CAT = "category_primary"
COL_KEY = "global_key"

PLACEHOLDERS = {
    "Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE", "<na>", "nan", "NaN"
}

STOPWORDS = {
    "and", "or", "the", "a", "an", "to", "for", "in", "of", "on", "with", "at", "by",
    "from", "is", "are", "as", "be", "you", "we", "our", "their", "they",
    "job", "jobs", "role", "vacancy", "hiring", "required", "requirement",
    "manager", "officer", "associate", "executive", "assistant", "senior", "junior",
}


# =========================
# HELPERS
# =========================
def _mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


def _mtime_iso(path: str) -> str:
    mt = _mtime(path)
    return datetime.fromtimestamp(mt).isoformat(timespec="seconds") if mt else "N/A"


def _empty_fig(title: str, subtitle: str = "No data available") -> go.Figure:
    fig = go.Figure()
    fig.update_layout(
        title=title,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{
            "text": subtitle,
            "xref": "paper",
            "yref": "paper",
            "showarrow": False,
            "x": 0.5,
            "y": 0.5,
            "font": {"size": 14},
        }],
        margin=dict(l=20, r=20, t=55, b=20),
    )
    return fig


def _read_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        return pd.read_csv(path, engine="python")


def _safe_load() -> pd.DataFrame:
    if not os.path.exists(LOCAL_MASTER_CSV):
        return pd.DataFrame()

    try:
        df = _read_csv(LOCAL_MASTER_CSV)
    except Exception:
        return pd.DataFrame()

    # ensure required columns exist
    required = [COL_TIME, COL_TITLE, COL_COMPANY, COL_LOCATION, COL_COUNTRY, COL_SOURCE, COL_CAT]
    for c in required:
        if c not in df.columns:
            df[c] = pd.NA

    if COL_KEY not in df.columns:
        df[COL_KEY] = pd.NA

    # parse datetime
    df[COL_TIME] = pd.to_datetime(df[COL_TIME], errors="coerce")

    # normalize text columns
    for c in [COL_TITLE, COL_COMPANY, COL_LOCATION, COL_COUNTRY, COL_SOURCE, COL_CAT, COL_KEY]:
        df[c] = df[c].astype("string").str.strip()
        df.loc[df[c].isin(PLACEHOLDERS), c] = pd.NA

    # fill category-like with Unknown
    for c in [COL_COMPANY, COL_LOCATION, COL_COUNTRY, COL_SOURCE, COL_CAT]:
        df[c] = df[c].fillna("Unknown")

    df[COL_TITLE] = df[COL_TITLE].fillna("")

    return df


def _apply_date_range(df: pd.DataFrame, range_value: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.dropna(subset=[COL_TIME]).copy()
    if out.empty:
        return out

    if range_value == "all":
        return out

    days = int(range_value)
    max_dt = out[COL_TIME].max()
    if pd.isna(max_dt):
        return out

    cutoff = max_dt - pd.Timedelta(days=days)
    return out[out[COL_TIME] >= cutoff]


def _day_bucket(df: pd.DataFrame) -> pd.Series:
    return df[COL_TIME].dt.floor("d")


def _count_daily(df: pd.DataFrame, mode: str = "unique") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["day", "jobs"])

    temp = df.dropna(subset=[COL_TIME]).copy()
    if temp.empty:
        return pd.DataFrame(columns=["day", "jobs"])

    temp["day"] = _day_bucket(temp)

    if mode == "rows":
        agg = temp.groupby("day").size().reset_index(name="jobs")
        return agg.sort_values("day")

    # unique by global_key if available
    if COL_KEY in temp.columns and temp[COL_KEY].notna().any():
        agg = temp.groupby("day")[COL_KEY].nunique().reset_index(name="jobs")
    else:
        agg = temp.groupby("day").size().reset_index(name="jobs")

    return agg.sort_values("day")


def _top_n_series(df: pd.DataFrame, col: str, n: int = 10) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return pd.DataFrame(columns=[col, "count"])

    temp = df.copy()
    temp[col] = temp[col].fillna("Unknown").astype("string").str.strip()
    counts = temp[col].value_counts().head(n).reset_index()
    counts.columns = [col, "count"]
    return counts


def _tokenize_titles(df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    if df.empty or COL_TITLE not in df.columns:
        return pd.DataFrame(columns=["keyword", "count"])

    text = " ".join(df[COL_TITLE].astype("string").fillna("").tolist()).lower()
    # keep letters/numbers plus + # . net etc. (simple)
    tokens = re.findall(r"[a-z0-9\+#\.]{2,}", text)

    cleaned = []
    for t in tokens:
        if t in STOPWORDS:
            continue
        if t.isdigit():
            continue
        cleaned.append(t)

    if not cleaned:
        return pd.DataFrame(columns=["keyword", "count"])

    s = pd.Series(cleaned)
    out = s.value_counts().head(top_n).reset_index()
    out.columns = ["keyword", "count"]
    return out


# =========================
# DASH APP
# =========================
app = Dash(__name__)

app.layout = html.Div(
    style={"padding": "14px", "fontFamily": "Arial", "maxWidth": "1300px", "margin": "0 auto"},
    children=[
        html.H2("Job Market Highlights (Daily) — Local CSV"),

        html.Div(
            style={"display": "flex", "gap": "10px", "alignItems": "center", "flexWrap": "wrap"},
            children=[
                html.Button("Refresh now", id="btn-refresh", n_clicks=0,
                            style={"padding": "10px 12px", "borderRadius": 10}),
                html.Div(
                    style={"minWidth": 220},
                    children=[
                        html.Div("Date range", style={"fontSize": 12, "opacity": 0.7}),
                        dcc.Dropdown(
                            id="range",
                            options=[
                                {"label": "Last 7 days", "value": "7"},
                                {"label": "Last 30 days", "value": "30"},
                                {"label": "Last 90 days", "value": "90"},
                                {"label": "All", "value": "all"},
                            ],
                            value="30",
                            clearable=False,
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": 260},
                    children=[
                        html.Div("Counting mode", style={"fontSize": 12, "opacity": 0.7}),
                        dcc.Dropdown(
                            id="count-mode",
                            options=[
                                {"label": "Unique jobs (global_key)", "value": "unique"},
                                {"label": "Raw rows (every scrape row)", "value": "rows"},
                            ],
                            value="unique",
                            clearable=False,
                        ),
                    ],
                ),
                html.Div(id="status", style={"whiteSpace": "pre-wrap", "fontSize": 12, "opacity": 0.85}),
            ],
        ),

        dcc.Interval(id="poll", interval=REFRESH_SECONDS * 1000, n_intervals=0),
        dcc.Store(id="store-mtime", data=None),

        html.Hr(),

        # Responsive grid: 2 columns on desktop, 1 on mobile
        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr",
                "gap": "12px",
            },
            children=[
                dcc.Graph(id="daily_trend"),
            ],
        ),

        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1fr",
                "gap": "12px",
            },
            children=[
                dcc.Graph(id="top_locations"),
                dcc.Graph(id="top_companies"),
                dcc.Graph(id="it_nonit"),
                dcc.Graph(id="portal_share"),
                dcc.Graph(id="countries"),
                dcc.Graph(id="keywords"),
            ],
        ),

        html.Div(
            style={"fontSize": 12, "opacity": 0.75, "marginTop": 10},
            children=[
                html.Div(f"Reading: {LOCAL_MASTER_CSV}"),
                html.Div("X = scraped_at (day buckets), Y = #jobs"),
            ],
        ),
    ],
)


@callback(
    Output("daily_trend", "figure"),
    Output("top_locations", "figure"),
    Output("top_companies", "figure"),
    Output("it_nonit", "figure"),
    Output("portal_share", "figure"),
    Output("countries", "figure"),
    Output("keywords", "figure"),
    Output("status", "children"),
    Output("store-mtime", "data"),
    Input("poll", "n_intervals"),
    Input("btn-refresh", "n_clicks"),
    Input("range", "value"),
    Input("count-mode", "value"),
    State("store-mtime", "data"),
)
def update_all(n_intervals, _refresh, range_value, count_mode, prev_mtime):
    cur_mtime = _mtime(LOCAL_MASTER_CSV)
    df = _safe_load()

    if df.empty or df[COL_TIME].isna().all():
        msg = (
            f"❌ No usable data loaded\n"
            f"File: {LOCAL_MASTER_CSV}\n"
            f"Modified: {_mtime_iso(LOCAL_MASTER_CSV)}\n"
            f"Tick: {n_intervals} | Refresh: {REFRESH_SECONDS}s"
        )
        empty = _empty_fig("No data")
        return empty, empty, empty, empty, empty, empty, empty, msg, cur_mtime

    base = _apply_date_range(df, range_value or "30")

    # 1) Daily trend
    trend = _count_daily(base, mode=count_mode or "unique")
    if trend.empty:
        f_trend = _empty_fig("Daily job count", "No points in selected range")
    else:
        f_trend = px.line(trend, x="day", y="jobs", markers=True, title="Daily job count")
        f_trend.update_layout(margin=dict(l=20, r=20, t=55, b=20))
        f_trend.update_xaxes(title_text="Day")
        f_trend.update_yaxes(title_text="# jobs")

    # 2) Top locations
    loc = _top_n_series(base, COL_LOCATION, n=10)
    f_loc = _empty_fig("Top locations") if loc.empty else px.bar(loc, x="count", y=COL_LOCATION, orientation="h", title="Top 10 locations")
    f_loc.update_layout(margin=dict(l=20, r=20, t=55, b=20), yaxis={"categoryorder": "total ascending"})

    # 3) Top companies
    comp = _top_n_series(base, COL_COMPANY, n=10)
    f_comp = _empty_fig("Top companies") if comp.empty else px.bar(comp, x="count", y=COL_COMPANY, orientation="h", title="Top 10 companies")
    f_comp.update_layout(margin=dict(l=20, r=20, t=55, b=20), yaxis={"categoryorder": "total ascending"})

    # 4) IT vs Non-IT
    cat = _top_n_series(base, COL_CAT, n=10)
    f_cat = _empty_fig("IT vs Non-IT") if cat.empty else px.bar(cat, x=COL_CAT, y="count", title="IT vs Non-IT (category_primary)")
    f_cat.update_layout(margin=dict(l=20, r=20, t=55, b=20), xaxis_title="Category", yaxis_title="Count")

    # 5) Portal share
    src = _top_n_series(base, COL_SOURCE, n=20)
    f_src = _empty_fig("Portal share") if src.empty else px.bar(src, x=COL_SOURCE, y="count", title="Portal share")
    f_src.update_layout(margin=dict(l=20, r=20, t=55, b=20), xaxis_title="Portal", yaxis_title="Count")

    # 6) Countries
    ctry = _top_n_series(base, COL_COUNTRY, n=20)
    f_ctry = _empty_fig("Countries distribution") if ctry.empty else px.bar(ctry, x=COL_COUNTRY, y="count", title="Countries distribution")
    f_ctry.update_layout(margin=dict(l=20, r=20, t=55, b=20), xaxis_title="Country", yaxis_title="Count")

    # 7) Keywords
    kw = _tokenize_titles(base, top_n=15)
    f_kw = _empty_fig("Trending keywords") if kw.empty else px.bar(kw, x="keyword", y="count", title="Trending title keywords")
    f_kw.update_layout(margin=dict(l=20, r=20, t=55, b=20), xaxis_title="Keyword", yaxis_title="Count")

    msg = (
        f"✅ Updated\n"
        f"File: {LOCAL_MASTER_CSV}\n"
        f"Modified: {_mtime_iso(LOCAL_MASTER_CSV)}\n"
        f"Rows loaded: {len(df)} | Rows in range: {len(base)}\n"
        f"Range: {range_value} | Count: {count_mode}\n"
        f"Tick: {n_intervals} | Refresh: {REFRESH_SECONDS}s"
    )
    return f_trend, f_loc, f_comp, f_cat, f_src, f_ctry, f_kw, msg, cur_mtime


if __name__ == "__main__":
    print("RUNNING:", __file__)
    app.run(debug=True, host="127.0.0.1", port=8051)