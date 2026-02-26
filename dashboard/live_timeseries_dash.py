# dashboard/live_timeseries_dash.py
from __future__ import annotations

import os
import time
import shutil
from typing import Optional, List, Dict, Tuple

import pandas as pd
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, State, callback
from dash.exceptions import PreventUpdate

# =========================
# CONFIG
# =========================
MASTER_CSV = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx/jobs_master.csv"

LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"
LOCAL_MASTER_CSV = os.path.join(LOCAL_CACHE_DIR, "jobs_master_local.csv")

REFRESH_SECONDS = 20

COL_TIME = "scraped_at"
COL_DAY = "day"
COL_KEY = "global_key"

COL_SOURCE = "source"
COL_COUNTRY = "country"

# ✅ You said Designation and Category are separate
# IMPORTANT: set these to match your CSV headers exactly
COL_DESIGNATION = "designation"
COL_CATEGORY = "category_primary"

COL_WORKMODE = "work_mode"
COL_EMP = "employment_type"
COL_TITLE = "title"

# ✅ Domains exist in data but MUST NOT show as normal filters.
# They ONLY appear inside the Designation nesting block (hidden until designation selected)
COL_D1 = "domain_l1"
COL_D2 = "domain_l2"
COL_D3 = "domain_l3"

ID_D1 = "dd-d1"
ID_D2 = "dd-d2"
ID_D3 = "dd-d3"
ID_CATEGORY = "dd-category"

PLACEHOLDERS = {
    "Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE",
    "<na>", "<NA>", "nan", "NaN", "NULL", "null"
}

# ✅ Compare-by can include domains (optional).
# If you also want to REMOVE domains from Compare-by, delete domain_l1/l2/l3 below.
COMPARE_MAP: Dict[str, Tuple[str, str]] = {
    "none": ("None (single line)", ""),
    "source": ("Portal source", COL_SOURCE),
    "country": ("Country", COL_COUNTRY),
    "designation": ("Designation", COL_DESIGNATION),
    "category": ("Category", COL_CATEGORY),
    "work_mode": ("Work mode", COL_WORKMODE),
    "employment_type": ("Employment type", COL_EMP),
    "domain_l1": ("Domain (L1)", COL_D1),
    "domain_l2": ("Domain (L2)", COL_D2),
    "domain_l3": ("Domain (L3)", COL_D3),
}

_last_good_df: Optional[pd.DataFrame] = None


# =========================
# SAFE LOADERS (OneDrive friendly)
# =========================
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _copy_to_local_cache(src_path: str, dst_path: str) -> bool:
    try:
        _ensure_dir(os.path.dirname(dst_path))
        tmp = dst_path + f".tmp_{int(time.time())}"
        shutil.copy2(src_path, tmp)
        os.replace(tmp, dst_path)
        return True
    except Exception as e:
        print(f"[WARN] Could not copy to local cache:\n  src={src_path}\n  dst={dst_path}\n  err={e}")
        return False


def _read_csv_safe(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"[WARN] read_csv failed, retry python engine: {e}")
        return pd.read_csv(path, engine="python")


def _safe_load_master(master_csv_path: str) -> pd.DataFrame:
    global _last_good_df

    if not os.path.exists(master_csv_path):
        print(f"[WARN] master CSV not found: {master_csv_path}")
        return pd.DataFrame()

    _copy_to_local_cache(master_csv_path, LOCAL_MASTER_CSV)

    try:
        df = _read_csv_safe(LOCAL_MASTER_CSV)
        _last_good_df = df
        return df
    except Exception as e:
        print(f"[ERROR] Failed reading local cached CSV: {e}")
        if _last_good_df is not None:
            print("[WARN] Using last known good dataframe (fallback).")
            return _last_good_df.copy()
        return pd.DataFrame()


# =========================
# HELPERS
# =========================
def _clean_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace(list(PLACEHOLDERS), pd.NA)
    s = s.replace("", pd.NA)
    return s


def _prep_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    required = [
        COL_SOURCE, COL_COUNTRY,
        COL_DESIGNATION, COL_CATEGORY,
        COL_WORKMODE, COL_EMP,
        COL_TITLE, COL_TIME,
        COL_D1, COL_D2, COL_D3
    ]
    for c in required:
        if c not in df.columns:
            df[c] = pd.NA

    # time -> day
    df[COL_TIME] = pd.to_datetime(df[COL_TIME], errors="coerce", utc=True).dt.tz_convert(None)
    df[COL_DAY] = df[COL_TIME].dt.floor("D")
    df = df.dropna(subset=[COL_DAY])

    for c in [
        COL_SOURCE, COL_COUNTRY, COL_DESIGNATION, COL_CATEGORY,
        COL_WORKMODE, COL_EMP, COL_D1, COL_D2, COL_D3
    ]:
        df[c] = _clean_series(df[c])

    df[COL_TITLE] = df[COL_TITLE].astype(str)

    if COL_KEY not in df.columns:
        df[COL_KEY] = pd.NA

    return df


def _sorted_unique(series: pd.Series) -> List[str]:
    vals = series.dropna().astype(str).unique().tolist()
    vals = [v.strip() for v in vals if v.strip() and v.strip() not in PLACEHOLDERS]
    return sorted(set(vals), key=lambda x: x.lower())


def _apply_filters(
    df: pd.DataFrame,
    sources: Optional[List[str]],
    countries: Optional[List[str]],
    designation: Optional[List[str]],
    category: Optional[List[str]],
    d1: Optional[str],
    d2: Optional[List[str]],
    d3: Optional[List[str]],
    work_modes: Optional[List[str]],
    emps: Optional[List[str]],
    keyword: Optional[str],
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df

    def _isin(col: str, vals: Optional[List[str]]) -> None:
        nonlocal out
        if vals:
            out = out[out[col].isin(vals)]

    _isin(COL_SOURCE, sources)
    _isin(COL_COUNTRY, countries)
    _isin(COL_DESIGNATION, designation)
    _isin(COL_CATEGORY, category)
    _isin(COL_WORKMODE, work_modes)
    _isin(COL_EMP, emps)

    # domains (only applied if user selected them in nested block)
    if d1 and str(d1).strip():
        out = out[out[COL_D1] == str(d1).strip()]
    _isin(COL_D2, d2)
    _isin(COL_D3, d3)

    if keyword and keyword.strip():
        k = keyword.strip().lower()
        out = out[out[COL_TITLE].str.lower().str.contains(k, na=False)]

    return out


def _daily_count(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame({COL_DAY: [], "jobs": []})

    use_key = (COL_KEY in df.columns) and df[COL_KEY].notna().any()
    if use_key:
        g = df.groupby(COL_DAY)[COL_KEY].nunique().reset_index(name="jobs")
    else:
        g = df.groupby(COL_DAY).size().reset_index(name="jobs")

    return g.sort_values(COL_DAY)


def _daily_count_compare(df: pd.DataFrame, compare_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame({COL_DAY: [], compare_col: [], "jobs": []})

    use_key = (COL_KEY in df.columns) and df[COL_KEY].notna().any()
    if use_key:
        g = df.groupby([COL_DAY, compare_col])[COL_KEY].nunique().reset_index(name="jobs")
    else:
        g = df.groupby([COL_DAY, compare_col]).size().reset_index(name="jobs")

    return g.sort_values([COL_DAY, compare_col])


def _top_values_by_total(daily_cmp: pd.DataFrame, compare_col: str, top_n: int = 8) -> List[str]:
    if daily_cmp.empty:
        return []
    totals = daily_cmp.groupby(compare_col)["jobs"].sum().sort_values(ascending=False).head(top_n)
    return totals.index.astype(str).tolist()


def _fmt_multi_only_applied(label: str, values: Optional[List[str]]) -> Optional[str]:
    if not values:
        return None
    if len(values) <= 6:
        return f"{label}=" + ", ".join(values)
    return f"{label}=" + ", ".join(values[:6]) + f" (+{len(values)-6} more)"


def _fmt_single_only_applied(label: str, value: Optional[str]) -> Optional[str]:
    if not value or not str(value).strip():
        return None
    return f"{label}={str(value).strip()}"


def _build_filter_combo(
    sources, countries, designation, category, d1, d2, d3, work_modes, emps, keyword,
    compare_label: str, compare_vals: Optional[List[str]]
) -> str:
    parts: List[str] = []

    for maybe in [
        _fmt_multi_only_applied("source", sources),
        _fmt_multi_only_applied("country", countries),
        _fmt_multi_only_applied("designation", designation),
        _fmt_multi_only_applied("category", category),
        _fmt_single_only_applied("domain_l1", d1),
        _fmt_multi_only_applied("domain_l2", d2),
        _fmt_multi_only_applied("domain_l3", d3),
        _fmt_multi_only_applied("work_mode", work_modes),
        _fmt_multi_only_applied("employment_type", emps),
    ]:
        if maybe:
            parts.append(maybe)

    if keyword and keyword.strip():
        parts.append(f"keyword={keyword.strip()}")

    parts.append(f"compare_by={compare_label if compare_label else 'none'}")

    if compare_vals:
        shown = ", ".join(compare_vals[:8]) + ("..." if len(compare_vals) > 8 else "")
        parts.append(f"compare_vals={shown}")

    return " | ".join(parts) if parts else "no filters applied"


# =========================
# FIGURES (NO hoverdistance on Scatter)
# =========================
HOVER_TMPL = (
    "<b>Day</b>: %{x|%Y-%m-%d}<br>"
    "<b># of jobs</b>: %{y}<br>"
    "<b>Line</b>: %{customdata[0]}<br>"
    "<b>Filters</b>: %{customdata[1]}"
    "<extra></extra>"
)


def _apply_axes_spikes(fig: go.Figure) -> None:
    fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor")
    fig.update_yaxes(showspikes=True, spikemode="across", spikesnap="cursor")


def _fig_overall(daily: pd.DataFrame, filter_combo: str) -> go.Figure:
    fig = go.Figure()
    customdata = [["overall", filter_combo] for _ in range(len(daily))]

    fig.add_trace(go.Scatter(
        x=daily[COL_DAY],
        y=daily["jobs"],
        mode="lines+markers",
        name="overall",
        customdata=customdata,
        hovertemplate=HOVER_TMPL,
        marker=dict(size=7),
    ))

    fig.update_layout(
        title="Jobs per day (overall)",
        xaxis_title="Day",
        yaxis_title="Number of jobs",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
        legend=dict(orientation="h", y=1.02, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(rangeslider_visible=False)
    _apply_axes_spikes(fig)
    return fig


def _fig_compare(daily_cmp: pd.DataFrame, compare_label: str, compare_col: str, selected_vals: List[str], filter_combo: str) -> go.Figure:
    fig = go.Figure()

    for v in selected_vals:
        sub = daily_cmp[daily_cmp[compare_col] == v]
        if sub.empty:
            continue

        line_name = str(v)
        customdata = [[line_name, filter_combo] for _ in range(len(sub))]

        fig.add_trace(go.Scatter(
            x=sub[COL_DAY],
            y=sub["jobs"],
            mode="lines+markers",
            name=line_name,
            customdata=customdata,
            hovertemplate=HOVER_TMPL,
            marker=dict(size=7),
        ))

    fig.update_layout(
        title=f"Jobs per day (compare by: {compare_label})",
        xaxis_title="Day",
        yaxis_title="Number of jobs",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40),
        legend=dict(orientation="h", y=1.02, x=0, itemclick="toggle", itemdoubleclick="toggleothers"),
        hovermode="x unified",
    )
    fig.update_xaxes(rangeslider_visible=False)
    _apply_axes_spikes(fig)
    return fig


# =========================
# APP
# =========================
app = Dash(__name__)
server = app.server

CARD = {
    "background": "white",
    "border": "1px solid rgba(0,0,0,0.08)",
    "borderRadius": "14px",
    "padding": "12px",
    "boxShadow": "0 6px 18px rgba(0,0,0,0.05)",
}

app.layout = html.Div(
    style={"maxWidth": "1250px", "margin": "0 auto", "padding": "14px"},
    children=[
        html.Div(
            style={"display": "flex", "gap": "12px", "alignItems": "baseline", "flexWrap": "wrap"},
            children=[
                html.H2("Live Jobs Time Series", style={"margin": "0"}),
                html.Div(id="status", style={"marginLeft": "auto", "fontSize": "13px", "opacity": 0.9}),
            ],
        ),

        html.Div(style={"height": "10px"}),

        html.Div(
            style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "alignItems": "center"},
            children=[
                html.Button("Refresh graph", id="btn-refresh", n_clicks=0, style={"padding": "8px 12px"}),
                html.Button("Clear all filters", id="btn-clear", n_clicks=0, style={"padding": "8px 12px"}),
            ],
        ),

        html.Hr(),

        # ✅ Filters (Domains NOT shown here)
        html.Div(
            style=CARD,
            children=[
                html.Div(
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "repeat(auto-fit, minmax(260px, 1fr))",
                        "gap": "14px",
                        "alignItems": "end",
                    },
                    children=[
                        html.Div([html.Label("Portal source"),
                                  dcc.Dropdown(id="dd-source", multi=True, placeholder="Select portal source")]),
                        html.Div([html.Label("Country"),
                                  dcc.Dropdown(id="dd-country", multi=True, placeholder="Select country")]),

                        html.Div([html.Label("Designation"),
                                  dcc.Dropdown(id="dd-designation", multi=True, placeholder="Select designation")]),
                        html.Div([html.Label("Category"),
                                  dcc.Dropdown(id=ID_CATEGORY, multi=True, placeholder="Select category")]),

                        html.Div([html.Label("Work mode"),
                                  dcc.Dropdown(id="dd-work", multi=True, placeholder="Select work mode")]),
                        html.Div([html.Label("Employment type"),
                                  dcc.Dropdown(id="dd-emp", multi=True, placeholder="Select employment type")]),

                        html.Div(
                            [
                                html.Label("Title keyword"),
                                dcc.Input(
                                    id="in-keyword",
                                    type="text",
                                    placeholder="e.g., data, devops, analyst",
                                    value="",
                                    style={"width": "100%", "padding": "8px"},
                                ),
                            ]
                        ),
                    ],
                ),

                # ✅ DOMAINS are NOT normal filters.
                # They ONLY appear in this nested block under Designation.
                html.Div(
                    id="wrap-domains",
                    style={"display": "none", "marginTop": "12px"},
                    children=[
                        html.Hr(style={"margin": "14px 0"}),
                        html.Div(
                            style={
                                "display": "grid",
                                "gridTemplateColumns": "repeat(auto-fit, minmax(260px, 1fr))",
                                "gap": "14px",
                                "alignItems": "end",
                            },
                            children=[
                                html.Div([html.Label("Domain (L1) — depends on Designation"),
                                          dcc.Dropdown(id=ID_D1, multi=False, placeholder="Select domain L1")]),
                                html.Div([html.Label("Domain (L2) — depends on L1"),
                                          dcc.Dropdown(id=ID_D2, multi=True, placeholder="Select domain L2")]),
                                html.Div([html.Label("Domain (L3) — depends on L2"),
                                          dcc.Dropdown(id=ID_D3, multi=True, placeholder="Select domain L3")]),
                            ],
                        )
                    ],
                ),
            ],
        ),

        html.Div(style={"height": "12px"}),

        # Compare
        html.Div(
            style=CARD,
            children=[
                html.Div(
                    style={
                        "display": "grid",
                        "gridTemplateColumns": "repeat(auto-fit, minmax(360px, 1fr))",
                        "gap": "14px",
                        "alignItems": "end",
                    },
                    children=[
                        html.Div(
                            [
                                html.Label("Compare by"),
                                dcc.Dropdown(
                                    id="dd-compare-by",
                                    clearable=False,
                                    value="source",
                                    options=[{"label": COMPARE_MAP[k][0], "value": k} for k in COMPARE_MAP.keys()],
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                html.Label("Comparison values (multi-select)"),
                                dcc.Dropdown(
                                    id="dd-compare-vals",
                                    multi=True,
                                    placeholder="Select values to create multiple lines",
                                    options=[],
                                    value=[],
                                ),
                            ]
                        ),
                    ],
                )
            ],
        ),

        html.Div(style={"height": "12px"}),

        html.Div(
            style=CARD,
            children=[
                dcc.Graph(
                    id="timeseries",
                    config={"displayModeBar": True, "responsive": True},
                    style={"height": "560px"},
                ),
            ],
        ),

        dcc.Interval(id="poll", interval=REFRESH_SECONDS * 1000, n_intervals=0),
        dcc.Store(id="store-mtime", data=None),
    ],
)


# =========================
# MTIME POLL
# =========================
@callback(
    Output("store-mtime", "data"),
    Input("poll", "n_intervals"),
    State("store-mtime", "data"),
)
def poll_mtime(_n: int, prev: Optional[float]):
    try:
        m = os.path.getmtime(MASTER_CSV)
    except Exception:
        return prev
    if prev is None or m != prev:
        return m
    return prev


# =========================
# Show/Hide Domains (ONLY when designation selected)
# =========================
@callback(
    Output("wrap-domains", "style"),
    Output(ID_D1, "disabled"),
    Output(ID_D2, "disabled"),
    Output(ID_D3, "disabled"),
    Input("dd-designation", "value"),
)
def toggle_domain_visibility(designation_vals):
    has_designation = bool(designation_vals)
    if not has_designation:
        return {"display": "none", "marginTop": "12px"}, True, True, True
    return {"display": "block", "marginTop": "12px"}, False, False, False


# =========================
# Reset Domains when Designation cleared (avoid stale values)
# =========================
@callback(
    Output(ID_D1, "value", allow_duplicate=True),
    Output(ID_D2, "value", allow_duplicate=True),
    Output(ID_D3, "value", allow_duplicate=True),
    Input("dd-designation", "value"),
    prevent_initial_call=True,
)
def reset_domains_when_designation_cleared(designation_vals):
    if not designation_vals:
        return None, [], []
    raise PreventUpdate


# =========================
# Populate base filter options (Domains NOT included here)
# =========================
@callback(
    Output("dd-source", "options"),
    Output("dd-country", "options"),
    Output("dd-designation", "options"),
    Output(ID_CATEGORY, "options"),
    Output("dd-work", "options"),
    Output("dd-emp", "options"),
    Input("store-mtime", "data"),
)
def fill_filter_options(_mtime):
    df = _prep_df(_safe_load_master(MASTER_CSV))
    if df.empty:
        empty: List[Dict[str, str]] = []
        return empty, empty, empty, empty, empty, empty

    def opts(col: str):
        return [{"label": v, "value": v} for v in _sorted_unique(df[col])]

    return (
        opts(COL_SOURCE),
        opts(COL_COUNTRY),
        opts(COL_DESIGNATION),
        opts(COL_CATEGORY),
        opts(COL_WORKMODE),
        opts(COL_EMP),
    )


# =========================
# Domain L1 options (depends on Designation)
# =========================
@callback(
    Output(ID_D1, "options"),
    Output(ID_D1, "value"),
    Input("store-mtime", "data"),
    Input("dd-source", "value"),
    Input("dd-country", "value"),
    Input("dd-designation", "value"),
    Input("dd-work", "value"),
    Input("dd-emp", "value"),
    Input("in-keyword", "value"),
    State(ID_D1, "value"),
)
def fill_domain_l1(_mtime, sources, countries, designation, work_modes, emps, keyword, current_d1):
    df = _prep_df(_safe_load_master(MASTER_CSV))
    if df.empty or not designation:
        return [], None

    # NOTE: Domain nesting is under Designation ONLY (Category does not affect domain lists)
    df_f = _apply_filters(df, sources, countries, designation, None, None, None, None, work_modes, emps, keyword)
    values = _sorted_unique(df_f[COL_D1]) if not df_f.empty else []
    options = [{"label": v, "value": v} for v in values]

    return options, current_d1 if current_d1 in values else None


# =========================
# Domain L2 options (depends on L1)
# =========================
@callback(
    Output(ID_D2, "options"),
    Output(ID_D2, "value"),
    Input(ID_D1, "value"),
    Input("store-mtime", "data"),
    Input("dd-source", "value"),
    Input("dd-country", "value"),
    Input("dd-designation", "value"),
    Input("dd-work", "value"),
    Input("dd-emp", "value"),
    Input("in-keyword", "value"),
    State(ID_D2, "value"),
)
def fill_domain_l2(d1, _mtime, sources, countries, designation, work_modes, emps, keyword, current_vals):
    df = _prep_df(_safe_load_master(MASTER_CSV))
    if df.empty or not designation or not d1:
        return [], []

    df_f = _apply_filters(df, sources, countries, designation, None, d1, None, None, work_modes, emps, keyword)
    values = _sorted_unique(df_f[COL_D2]) if not df_f.empty else []
    options = [{"label": v, "value": v} for v in values]

    current_vals = current_vals or []
    current_vals = [v for v in current_vals if v in values]
    return options, current_vals


# =========================
# Domain L3 options (depends on L1 + L2)
# =========================
@callback(
    Output(ID_D3, "options"),
    Output(ID_D3, "value"),
    Input(ID_D1, "value"),
    Input(ID_D2, "value"),
    Input("store-mtime", "data"),
    Input("dd-source", "value"),
    Input("dd-country", "value"),
    Input("dd-designation", "value"),
    Input("dd-work", "value"),
    Input("dd-emp", "value"),
    Input("in-keyword", "value"),
    State(ID_D3, "value"),
)
def fill_domain_l3(d1, d2_vals, _mtime, sources, countries, designation, work_modes, emps, keyword, current_vals):
    df = _prep_df(_safe_load_master(MASTER_CSV))
    if df.empty or not designation or not d1 or not d2_vals:
        return [], []

    df_f = _apply_filters(df, sources, countries, designation, None, d1, d2_vals, None, work_modes, emps, keyword)
    values = _sorted_unique(df_f[COL_D3]) if not df_f.empty else []
    options = [{"label": v, "value": v} for v in values]

    current_vals = current_vals or []
    current_vals = [v for v in current_vals if v in values]
    return options, current_vals


# =========================
# Compare values options (fix: always returns options)
# =========================
@callback(
    Output("dd-compare-vals", "options"),
    Output("dd-compare-vals", "value"),
    Input("dd-compare-by", "value"),
    Input("store-mtime", "data"),
    Input("dd-source", "value"),
    Input("dd-country", "value"),
    Input("dd-designation", "value"),
    Input(ID_CATEGORY, "value"),
    Input(ID_D1, "value"),
    Input(ID_D2, "value"),
    Input(ID_D3, "value"),
    Input("dd-work", "value"),
    Input("dd-emp", "value"),
    Input("in-keyword", "value"),
    State("dd-compare-vals", "value"),
)
def fill_compare_values(compare_by_key, _mtime, sources, countries, designation, category, d1, d2, d3, work_modes, emps, keyword, current_vals):
    compare_label, compare_col = COMPARE_MAP.get(compare_by_key, ("None (single line)", ""))
    if not compare_col:
        return [], []

    df = _prep_df(_safe_load_master(MASTER_CSV))
    if df.empty:
        return [], []

    df_f = _apply_filters(df, sources, countries, designation, category, d1, d2, d3, work_modes, emps, keyword)

    values = _sorted_unique(df_f[compare_col]) if not df_f.empty else []
    if not values:
        values = _sorted_unique(df[compare_col])

    options = [{"label": v, "value": v} for v in values]

    current_vals = current_vals or []
    current_vals = [v for v in current_vals if v in values]

    if not current_vals and values:
        base_df = df_f if not df_f.empty else df
        daily_cmp = _daily_count_compare(base_df, compare_col)
        current_vals = _top_values_by_total(daily_cmp, compare_col, top_n=8)

    return options, current_vals


# =========================
# Clear filters (allow_duplicate for domains + compare vals)
# =========================
@callback(
    Output("dd-source", "value"),
    Output("dd-country", "value"),
    Output("dd-designation", "value"),
    Output(ID_CATEGORY, "value"),
    Output("dd-work", "value"),
    Output("dd-emp", "value"),
    Output("in-keyword", "value"),

    Output(ID_D1, "value", allow_duplicate=True),
    Output(ID_D2, "value", allow_duplicate=True),
    Output(ID_D3, "value", allow_duplicate=True),

    Output("dd-compare-by", "value"),
    Output("dd-compare-vals", "value", allow_duplicate=True),

    Input("btn-clear", "n_clicks"),
    prevent_initial_call=True,
)
def clear_all(_n):
    return None, None, None, None, None, None, "", None, [], [], "source", []


# =========================
# Main figure
# =========================
@callback(
    Output("timeseries", "figure"),
    Output("status", "children"),
    Input("store-mtime", "data"),
    Input("btn-refresh", "n_clicks"),
    Input("dd-source", "value"),
    Input("dd-country", "value"),
    Input("dd-designation", "value"),
    Input(ID_CATEGORY, "value"),
    Input("dd-work", "value"),
    Input("dd-emp", "value"),
    Input("in-keyword", "value"),
    Input(ID_D1, "value"),
    Input(ID_D2, "value"),
    Input(ID_D3, "value"),
    Input("dd-compare-by", "value"),
    Input("dd-compare-vals", "value"),
)
def update_figure(_mtime, _n_refresh, sources, countries, designation, category, work_modes, emps, keyword, d1, d2, d3, compare_by_key, compare_vals):
    df = _prep_df(_safe_load_master(MASTER_CSV))
    total_rows = len(df)

    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="No data loaded", template="plotly_white")
        return fig, f"No data | Poll: {REFRESH_SECONDS}s"

    df_f = _apply_filters(df, sources, countries, designation, category, d1, d2, d3, work_modes, emps, keyword)
    filtered_rows = len(df_f)

    compare_label, compare_col = COMPARE_MAP.get(compare_by_key, ("None (single line)", ""))
    filter_combo = _build_filter_combo(
        sources, countries, designation, category, d1, d2, d3, work_modes, emps, keyword,
        compare_label if compare_col else "none",
        compare_vals or [],
    )

    if filtered_rows == 0:
        fig = go.Figure()
        fig.update_layout(title="No rows match current filters", template="plotly_white")
        return fig, f"Rows: 0 / {total_rows:,} | Filters too strict | Poll: {REFRESH_SECONDS}s"

    if not compare_col:
        daily = _daily_count(df_f)
        fig = _fig_overall(daily, filter_combo)
        return fig, f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: none | Poll: {REFRESH_SECONDS}s"

    compare_vals = compare_vals or []
    if not compare_vals:
        daily = _daily_count(df_f)
        fig = _fig_overall(daily, filter_combo)
        return fig, f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: {compare_label} | Lines: 0 | Poll: {REFRESH_SECONDS}s"

    daily_cmp = _daily_count_compare(df_f, compare_col)
    fig = _fig_compare(daily_cmp, compare_label, compare_col, compare_vals, filter_combo)
    return fig, f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: {compare_label} | Lines: {len(compare_vals)} | Poll: {REFRESH_SECONDS}s"


if __name__ == "__main__":
    app.run(debug=True, port=8050)