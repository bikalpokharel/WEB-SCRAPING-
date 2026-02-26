import os
import pandas as pd
from datetime import datetime

import dash
from dash import dcc, html, Input, Output, State
import plotly.express as px


# =========================
# CONFIG
# =========================
DATA_DIR = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"
MASTER_CSV = os.path.join(DATA_DIR, "jobs_master.csv")

REFRESH_MS = 60_000  # 60 seconds (live refresh)

DATE_COL = "scraped_at"
UNKNOWN = "Unknown"

# Filters available in your master schema
FILTER_FIELDS = {
    "country": "Country",
    "category_primary": "IT / Non-IT",
    "source": "Portal Source",
    "employment_type": "Employment Type",
    "work_mode": "Work Mode",
    "position": "Designation / Position",
    "type": "Job Type",
    "company": "Company",
    "location": "Location",
}

DEFAULT_COMPARE_BY = "source"


# =========================
# DATA LOADING
# =========================
def load_master_csv() -> pd.DataFrame:
    if not os.path.exists(MASTER_CSV):
        return pd.DataFrame()

    df = pd.read_csv(MASTER_CSV)

    # Ensure date column exists
    if DATE_COL not in df.columns:
        return pd.DataFrame()

    # Parse scraped_at safely
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    # Drop rows without scraped_at (cannot plot on time axis)
    df = df.dropna(subset=[DATE_COL]).copy()

    # Add daily bucket
    df["day"] = df[DATE_COL].dt.date.astype(str)  # string for clean plot labels

    # Clean missing values for filter fields
    for col in FILTER_FIELDS.keys():
        if col in df.columns:
            # Convert to string, trim, fill missing
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace(["", "None", "nan", "<NA>"], pd.NA)
            df[col] = df[col].fillna(UNKNOWN)

    return df


def get_dropdown_options(df: pd.DataFrame, col: str):
    if df.empty or col not in df.columns:
        return []

    vals = (
        df[col]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    # Sort with Unknown at bottom
    vals_sorted = sorted([v for v in vals if v != UNKNOWN])
    if UNKNOWN in vals:
        vals_sorted.append(UNKNOWN)

    return [{"label": v, "value": v} for v in vals_sorted]


# =========================
# DASH APP
# =========================
app = dash.Dash(__name__)
app.title = "Nepal Job Market - Live Dashboard"


app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "12px"},
    children=[
        html.H2("Live Jobs per Day (Master Dataset)", style={"marginBottom": "6px"}),
        html.Div(
            id="last-updated",
            style={"fontSize": "14px", "opacity": "0.75", "marginBottom": "12px"},
        ),

        # Auto refresh trigger
        dcc.Interval(id="interval", interval=REFRESH_MS, n_intervals=0),

        # Store data in-memory
        dcc.Store(id="master-data"),

        # CONTROLS
        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                "gap": "10px",
                "marginBottom": "12px",
            },
            children=[
                html.Div(
                    children=[
                        html.Label("Compare by (multi-line)", style={"fontWeight": "600"}),
                        dcc.Dropdown(
                            id="compare-by",
                            options=[{"label": label, "value": key} for key, label in FILTER_FIELDS.items()],
                            value=DEFAULT_COMPARE_BY,
                            clearable=False,
                        ),
                    ]
                ),

                html.Div(
                    children=[
                        html.Label("Compare values (multi-select)", style={"fontWeight": "600"}),
                        dcc.Dropdown(
                            id="compare-values",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select values to compare (optional)",
                        ),
                    ]
                ),

                html.Div(
                    children=[
                        html.Label("Filter: Country", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-country", multi=True, placeholder="All countries"),
                    ]
                ),
                html.Div(
                    children=[
                        html.Label("Filter: IT / Non-IT", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-category_primary", multi=True, placeholder="All"),
                    ]
                ),
                html.Div(
                    children=[
                        html.Label("Filter: Source", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-source", multi=True, placeholder="All portals"),
                    ]
                ),
                html.Div(
                    children=[
                        html.Label("Filter: Employment Type", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-employment_type", multi=True, placeholder="All"),
                    ]
                ),
                html.Div(
                    children=[
                        html.Label("Filter: Work Mode", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-work_mode", multi=True, placeholder="All"),
                    ]
                ),
                html.Div(
                    children=[
                        html.Label("Filter: Position", style={"fontWeight": "600"}),
                        dcc.Dropdown(id="f-position", multi=True, placeholder="All"),
                    ]
                ),
            ],
        ),

        html.Div(
            style={"display": "flex", "gap": "10px", "marginBottom": "12px", "flexWrap": "wrap"},
            children=[
                html.Button("Clear filters", id="btn-clear", n_clicks=0),
                html.Button("Refresh now", id="btn-refresh", n_clicks=0),
            ],
        ),

        # GRAPH
        dcc.Graph(
            id="jobs-graph",
            config={"responsive": True},
            style={"height": "70vh"},
        ),

        html.Hr(),

        html.Div(
            style={"fontSize": "14px", "opacity": "0.85"},
            children=[
                html.Div("How it behaves:"),
                html.Ul(
                    [
                        html.Li("No compare values selected → shows ONE line = overall jobs/day (after filters)."),
                        html.Li("Compare values selected → shows MULTIPLE lines (different colors)."),
                        html.Li("All dropdowns are optional; leaving empty means 'All'."),
                        html.Li("Graph auto-refreshes from jobs_master.csv every 60 seconds (and on 'Refresh now')."),
                    ]
                ),
            ],
        ),
    ],
)


# =========================
# CALLBACKS
# =========================
@app.callback(
    Output("master-data", "data"),
    Output("last-updated", "children"),
    Input("interval", "n_intervals"),
    Input("btn-refresh", "n_clicks"),
)
def refresh_data(_n_intervals, _n_clicks):
    df = load_master_csv()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"Last loaded: {ts} | rows: {len(df)}"
    return df.to_dict("records"), msg


@app.callback(
    Output("compare-values", "options"),
    Input("compare-by", "value"),
    State("master-data", "data"),
)
def update_compare_values_options(compare_by, data):
    df = pd.DataFrame(data or [])
    if df.empty or not compare_by or compare_by not in df.columns:
        return []
    return get_dropdown_options(df, compare_by)


@app.callback(
    Output("f-country", "options"),
    Output("f-category_primary", "options"),
    Output("f-source", "options"),
    Output("f-employment_type", "options"),
    Output("f-work_mode", "options"),
    Output("f-position", "options"),
    Input("master-data", "data"),
)
def update_filter_options(data):
    df = pd.DataFrame(data or [])
    if df.empty:
        return [], [], [], [], [], []

    return (
        get_dropdown_options(df, "country"),
        get_dropdown_options(df, "category_primary"),
        get_dropdown_options(df, "source"),
        get_dropdown_options(df, "employment_type"),
        get_dropdown_options(df, "work_mode"),
        get_dropdown_options(df, "position"),
    )


@app.callback(
    Output("f-country", "value"),
    Output("f-category_primary", "value"),
    Output("f-source", "value"),
    Output("f-employment_type", "value"),
    Output("f-work_mode", "value"),
    Output("f-position", "value"),
    Output("compare-values", "value"),
    Input("btn-clear", "n_clicks"),
)
def clear_filters(n_clicks):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    return [], [], [], [], [], [], []


def apply_multi_filter(df: pd.DataFrame, col: str, selected: list):
    if not selected:
        return df
    if col not in df.columns:
        return df
    return df[df[col].isin(selected)].copy()


@app.callback(
    Output("jobs-graph", "figure"),
    Input("master-data", "data"),
    Input("compare-by", "value"),
    Input("compare-values", "value"),
    Input("f-country", "value"),
    Input("f-category_primary", "value"),
    Input("f-source", "value"),
    Input("f-employment_type", "value"),
    Input("f-work_mode", "value"),
    Input("f-position", "value"),
)
def update_graph(data, compare_by, compare_values, f_country, f_cat, f_source, f_emp, f_work, f_pos):
    df = pd.DataFrame(data or [])
    if df.empty:
        fig = px.line(title="No data loaded yet.")
        fig.update_layout(legend_title_text="Legend", margin=dict(l=20, r=20, t=50, b=20))
        return fig

    # Apply filters (these slice the dataset)
    df = apply_multi_filter(df, "country", f_country or [])
    df = apply_multi_filter(df, "category_primary", f_cat or [])
    df = apply_multi_filter(df, "source", f_source or [])
    df = apply_multi_filter(df, "employment_type", f_emp or [])
    df = apply_multi_filter(df, "work_mode", f_work or [])
    df = apply_multi_filter(df, "position", f_pos or [])

    # Ensure day exists
    if "day" not in df.columns:
        fig = px.line(title="Missing day column (scraped_at parse failed).")
        return fig

    # If user did not pick compare values => overall line
    if not compare_values:
        daily = df.groupby("day").size().reset_index(name="jobs")
        daily = daily.sort_values("day")
        fig = px.line(daily, x="day", y="jobs", markers=True, title="Jobs per Day (Overall)")
        fig.update_layout(legend_title_text="Legend", margin=dict(l=20, r=20, t=50, b=20))
        fig.update_xaxes(title="Day")
        fig.update_yaxes(title="Jobs")
        return fig

    # Multi-line comparison
    if not compare_by or compare_by not in df.columns:
        daily = df.groupby("day").size().reset_index(name="jobs")
        daily = daily.sort_values("day")
        fig = px.line(daily, x="day", y="jobs", markers=True, title="Jobs per Day (Overall)")
        return fig

    # Only compare selected values
    df_cmp = df[df[compare_by].isin(compare_values)].copy()

    daily_cmp = (
        df_cmp.groupby(["day", compare_by])
        .size()
        .reset_index(name="jobs")
        .sort_values("day")
    )

    title = f"Jobs per Day (Compare by: {FILTER_FIELDS.get(compare_by, compare_by)})"
    fig = px.line(
        daily_cmp,
        x="day",
        y="jobs",
        color=compare_by,
        markers=True,
        title=title,
    )
    fig.update_layout(legend_title_text="Comparison", margin=dict(l=20, r=20, t=50, b=20))
    fig.update_xaxes(title="Day")
    fig.update_yaxes(title="Jobs")
    return fig


# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8060)