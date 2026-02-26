# dashboard/live_timeseries_bokeh.py
from __future__ import annotations

import os
import time
import shutil
from typing import Optional, List, Dict, Tuple

import pandas as pd

from bokeh.io import curdoc
from bokeh.layouts import column, row, Spacer
from bokeh.models import (
    Select,
    MultiChoice,
    TextInput,
    Button,
    Div,
    HoverTool,
    DatetimeTickFormatter,
)
from bokeh.plotting import figure
from bokeh.palettes import Category10, Category20


# ============================================================
# CONFIG
# ============================================================
MASTER_CSV = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx/jobs_master.csv"
LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"
LOCAL_MASTER_CSV = os.path.join(LOCAL_CACHE_DIR, "jobs_master_local.csv")

POLL_SECONDS = 10  # auto-refresh when CSV updates (mtime)

COL_TIME = "scraped_at"
COL_DAY = "day"
COL_KEY = "global_key"

COL_SOURCE = "source"
COL_COUNTRY = "country"
COL_CAT = "category_primary"
COL_WORKMODE = "work_mode"
COL_EMP = "employment_type"
COL_TITLE = "title"

PLACEHOLDERS = {
    "Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE",
    "<na>", "<NA>", "nan", "NaN", "NULL", "null"
}

COMPARE_MAP: Dict[str, Tuple[str, str]] = {
    "none": ("None (overall)", ""),
    "source": ("Portal source", COL_SOURCE),
    "country": ("Country", COL_COUNTRY),
    "category": ("Category", COL_CAT),
    "domain_l1": ("Domain (L1)", "domain_l1"),
    "domain_l2": ("Domain (L2)", "domain_l2"),
    "work_mode": ("Work mode", COL_WORKMODE),
    "employment_type": ("Employment type", COL_EMP),
}

_last_good_df: Optional[pd.DataFrame] = None
_last_mtime: Optional[float] = None
_updating_widgets: bool = False


# ============================================================
# SAFE LOADERS (OneDrive-friendly)
# ============================================================
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
        print(f"[WARN] cache copy failed: {e}")
        return False


def _read_csv_safe(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except Exception as e:
        print(f"[WARN] read_csv failed: {e}")
        return pd.read_csv(path, engine="python")


def _safe_load_master() -> pd.DataFrame:
    global _last_good_df

    if not os.path.exists(MASTER_CSV):
        return pd.DataFrame()

    _copy_to_local_cache(MASTER_CSV, LOCAL_MASTER_CSV)

    try:
        df = _read_csv_safe(LOCAL_MASTER_CSV)
        _last_good_df = df
        return df
    except Exception as e:
        print(f"[ERROR] failed reading local cache: {e}")
        if _last_good_df is not None:
            return _last_good_df.copy()
        return pd.DataFrame()


# ============================================================
# DATA PREP
# ============================================================
def _clean_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace(list(PLACEHOLDERS), pd.NA)
    s = s.replace("", pd.NA)
    return s


def _prep_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    for c in [COL_SOURCE, COL_COUNTRY, COL_CAT, COL_WORKMODE, COL_EMP, COL_TITLE, COL_TIME]:
        if c not in df.columns:
            df[c] = pd.NA

    # scraped_at -> datetime (UTC) -> naive (for plotting)
    df[COL_TIME] = pd.to_datetime(df[COL_TIME], errors="coerce", utc=True).dt.tz_convert(None)
    df[COL_DAY] = df[COL_TIME].dt.floor("D")
    df = df.dropna(subset=[COL_DAY])

    for c in [COL_SOURCE, COL_COUNTRY, COL_CAT, COL_WORKMODE, COL_EMP]:
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
    sources: List[str],
    countries: List[str],
    cats: List[str],
    work_modes: List[str],
    emps: List[str],
    keyword: str,
) -> pd.DataFrame:
    out = df

    def _isin(col: str, vals: List[str]):
        nonlocal out
        if vals:
            out = out[out[col].isin(vals)]

    _isin(COL_SOURCE, sources)
    _isin(COL_COUNTRY, countries)
    _isin(COL_CAT, cats)
    _isin(COL_WORKMODE, work_modes)
    _isin(COL_EMP, emps)

    if keyword and keyword.strip():
        k = keyword.strip().lower()
        out = out[out[COL_TITLE].str.lower().str.contains(k, na=False)]

    return out


def _count_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame({COL_DAY: [], "jobs": []})

    use_key = (COL_KEY in df.columns) and df[COL_KEY].notna().any()
    if use_key:
        g = df.groupby(COL_DAY)[COL_KEY].nunique().reset_index(name="jobs")
    else:
        g = df.groupby(COL_DAY).size().reset_index(name="jobs")

    return g.sort_values(COL_DAY)


def _count_daily_compare(df: pd.DataFrame, compare_col: str) -> pd.DataFrame:
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


def _palette(n: int) -> List[str]:
    if n <= 10:
        return list(Category10[10])[:n]
    if n <= 20:
        return list(Category20[20])[:n]
    base = list(Category20[20])
    return [base[i % len(base)] for i in range(n)]


# ============================================================
# UI (Dropdown-only)
# ============================================================
title = Div(text="<div style='font-size:22px;font-weight:700;'>Live Jobs Time Series</div>")
status = Div(text="", styles={"font-size": "12px", "opacity": "0.85"})

btn_refresh = Button(label="Refresh", button_type="primary", width=110)
btn_clear = Button(label="Clear", button_type="warning", width=110)

# ✅ dropdown multi-select widgets
mc_source = MultiChoice(title="Source", options=[], value=[], width=300)
mc_country = MultiChoice(title="Country", options=[], value=[], width=300)
mc_cat = MultiChoice(title="Category", options=[], value=[], width=300)
mc_work = MultiChoice(title="Work mode", options=[], value=[], width=300)
mc_emp = MultiChoice(title="Employment type", options=[], value=[], width=300)

in_keyword = TextInput(title="Title keyword", value="", placeholder="e.g., data, devops, analyst", width=300)

dd_compare_by = Select(
    title="Compare by",
    value="none",
    options=[(k, v[0]) for k, v in COMPARE_MAP.items()],
    width=300,
)

mc_compare_vals = MultiChoice(title="Compare values", options=[], value=[], width=300)
mc_compare_vals.visible = False

compare_help = Div(
    text="<div style='font-size:12px;opacity:.8;'>Choose Compare by → then pick multiple values to show multiple lines.</div>"
)


# ============================================================
# PLOT
# ============================================================
p = figure(
    title="Jobs per day (overall)",
    x_axis_type="datetime",
    height=540,
    sizing_mode="stretch_width",
    tools="pan,wheel_zoom,box_zoom,reset,save",
    toolbar_location="right",
)
p.xaxis.formatter = DatetimeTickFormatter(days="%Y-%m-%d")
p.yaxis.axis_label = "Number of jobs"
p.xaxis.axis_label = "Day"
p.legend.location = "top_left"
p.legend.click_policy = "hide"

hover = HoverTool(
    tooltips=[("Day", "@x{%F}"), ("Jobs", "@y"), ("Series", "@series")],
    formatters={"@x": "datetime"},
)
p.add_tools(hover)

_renderers = []


def _clear_renderers():
    global _renderers
    for r in _renderers:
        try:
            p.renderers.remove(r)
        except Exception:
            pass
    _renderers = []


# ============================================================
# UPDATE LOGIC
# ============================================================
def load_filter_options(df: pd.DataFrame) -> None:
    mc_source.options = _sorted_unique(df[COL_SOURCE])
    mc_country.options = _sorted_unique(df[COL_COUNTRY])
    mc_cat.options = _sorted_unique(df[COL_CAT])
    mc_work.options = _sorted_unique(df[COL_WORKMODE])
    mc_emp.options = _sorted_unique(df[COL_EMP])


def _fill_compare_values(df_filtered: pd.DataFrame) -> None:
    key = dd_compare_by.value
    compare_label, compare_col = COMPARE_MAP.get(key, ("None", ""))

    if not compare_col:
        mc_compare_vals.options = []
        mc_compare_vals.value = []
        mc_compare_vals.visible = False
        return

    mc_compare_vals.visible = True
    values = _sorted_unique(df_filtered[compare_col])
    mc_compare_vals.options = values

    current = mc_compare_vals.value or []
    current = [v for v in current if v in values]

    if not current and values:
        daily_cmp = _count_daily_compare(df_filtered, compare_col)
        current = _top_values_by_total(daily_cmp, compare_col, top_n=8)

    mc_compare_vals.value = current


def update_plot() -> None:
    global _updating_widgets
    if _updating_widgets:
        return

    df_raw = _safe_load_master()
    df = _prep_df(df_raw)

    if df.empty:
        _clear_renderers()
        p.title.text = "Jobs per day (no data)"
        status.text = f"No data loaded. Watching: {os.path.basename(MASTER_CSV)} | Poll: {POLL_SECONDS}s"
        return

    df_f = _apply_filters(
        df=df,
        sources=mc_source.value,
        countries=mc_country.value,
        cats=mc_cat.value,
        work_modes=mc_work.value,
        emps=mc_emp.value,
        keyword=in_keyword.value,
    )

    # update compare values options based on filtered df
    _updating_widgets = True
    try:
        _fill_compare_values(df_f)
    finally:
        _updating_widgets = False

    compare_key = dd_compare_by.value
    compare_label, compare_col = COMPARE_MAP.get(compare_key, ("None", ""))

    _clear_renderers()

    total_rows = len(df)
    filtered_rows = len(df_f)

    # -------------------------
    # DEFAULT: SINGLE LINE
    # -------------------------
    if not compare_col:
        daily = _count_daily(df_f)
        p.title.text = "Jobs per day (overall)"
        p.legend.items = []

        r = p.line(daily[COL_DAY], daily["jobs"], line_width=3, alpha=0.9)
        r2 = p.circle(daily[COL_DAY], daily["jobs"], size=6, alpha=0.9)

        r.data_source.data["series"] = ["overall"] * len(daily)
        r2.data_source.data["series"] = ["overall"] * len(daily)

        _renderers.extend([r, r2])

        status.text = f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: none | Auto refresh: {POLL_SECONDS}s"
        return

    # -------------------------
    # MULTI-LINE COMPARISON
    # -------------------------
    selected_vals = mc_compare_vals.value or []
    daily_cmp = _count_daily_compare(df_f, compare_col)

    if not selected_vals:
        daily = _count_daily(df_f)
        p.title.text = f"Jobs per day (compare by: {compare_label}) [no values selected]"
        p.legend.items = []

        r = p.line(daily[COL_DAY], daily["jobs"], line_width=3, alpha=0.9)
        r2 = p.circle(daily[COL_DAY], daily["jobs"], size=6, alpha=0.9)

        r.data_source.data["series"] = ["overall"] * len(daily)
        r2.data_source.data["series"] = ["overall"] * len(daily)

        _renderers.extend([r, r2])
        status.text = f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: {compare_label} | values: 0"
        return

    p.title.text = f"Jobs per day (compare by: {compare_label})"
    p.legend.items = []

    colors = _palette(len(selected_vals))

    for v, c in zip(selected_vals, colors):
        sub = daily_cmp[daily_cmp[compare_col] == v]
        if sub.empty:
            continue

        r = p.line(sub[COL_DAY], sub["jobs"], line_width=3, color=c, legend_label=str(v), alpha=0.9)
        r2 = p.circle(sub[COL_DAY], sub["jobs"], size=6, color=c, alpha=0.9)

        r.data_source.data["series"] = [str(v)] * len(sub)
        r2.data_source.data["series"] = [str(v)] * len(sub)

        _renderers.extend([r, r2])

    status.text = f"Rows: {filtered_rows:,} / {total_rows:,} | Compare: {compare_label} | values: {len(selected_vals)} | Auto refresh: {POLL_SECONDS}s"


# ============================================================
# EVENTS
# ============================================================
def on_refresh():
    update_plot()


def on_clear():
    global _updating_widgets
    _updating_widgets = True
    try:
        mc_source.value = []
        mc_country.value = []
        mc_cat.value = []
        mc_work.value = []
        mc_emp.value = []
        in_keyword.value = ""
        dd_compare_by.value = "none"
        mc_compare_vals.options = []
        mc_compare_vals.value = []
        mc_compare_vals.visible = False
    finally:
        _updating_widgets = False

    update_plot()


def on_any_change(attr, old, new):
    update_plot()


btn_refresh.on_click(on_refresh)
btn_clear.on_click(on_clear)

for w in [mc_source, mc_country, mc_cat, mc_work, mc_emp, in_keyword, dd_compare_by, mc_compare_vals]:
    w.on_change("value", on_any_change)


# ============================================================
# AUTO REFRESH WHEN CSV UPDATES
# ============================================================
def poll_file_changes():
    global _last_mtime
    try:
        m = os.path.getmtime(MASTER_CSV)
    except Exception:
        return

    if _last_mtime is None:
        _last_mtime = m
        return

    if m != _last_mtime:
        _last_mtime = m
        update_plot()


# ============================================================
# INIT
# ============================================================
df_init = _prep_df(_safe_load_master())
if not df_init.empty:
    load_filter_options(df_init)

update_plot()

left_panel = column(
    Div(text="<b>Filters</b>"),
    mc_source,
    mc_country,
    mc_cat,
    mc_work,
    mc_emp,
    in_keyword,
    Spacer(height=8),
    Div(text="<b>Comparison</b>"),
    compare_help,
    dd_compare_by,
    mc_compare_vals,
    Spacer(height=10),
    row(btn_refresh, btn_clear),
    sizing_mode="fixed",
    width=340,
)

layout = column(
    column(title, status, sizing_mode="stretch_width"),
    row(left_panel, Spacer(width=12), p, sizing_mode="stretch_width"),
    sizing_mode="stretch_width",
)

curdoc().add_root(layout)
curdoc().title = "Live Jobs Dashboard (Bokeh)"
curdoc().add_periodic_callback(poll_file_changes, POLL_SECONDS * 1000)