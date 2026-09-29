"""
Creates the Superset side of the project through the Superset REST API:
Snowflake database -> datasets -> charts -> "Weather Prediction Analytics" dashboard
(with City and Date Range native filters). It is safe to re-run: objects with the
same names are deleted and re-created.

Usage (Superset from docker-compose must be up on :8088):
    python superset/bootstrap_dashboard.py --account <ORG-ACCOUNT> --user <SNOWFLAKE_USER>
"""
import argparse, json
import requests

p = argparse.ArgumentParser()
p.add_argument("--url", default="http://localhost:8088")
p.add_argument("--account", required=True)
p.add_argument("--user", required=True)
p.add_argument("--superset-user", default="admin")
p.add_argument("--superset-password", default="admin")
args = p.parse_args()

S = requests.Session()
tok = S.post(f"{args.url}/api/v1/security/login", json={
    "username": args.superset_user, "password": args.superset_password,
    "provider": "db", "refresh": True}).json()["access_token"]
S.headers["Authorization"] = f"Bearer {tok}"
S.headers["X-CSRFToken"] = S.get(f"{args.url}/api/v1/security/csrf_token/").json()["result"]
S.headers["Referer"] = args.url


def api(method, path, **kw):
    r = S.request(method, f"{args.url}/api/v1/{path}", **kw)
    if not r.ok:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json()


def find(kind, col, value):
    q = f"(filters:!((col:{col},opr:eq,value:'{value}')))"
    return [o["id"] for o in api("GET", f"{kind}/?q={q}")["result"]]


# ---- clean previous run -------------------------------------------------
DASH = "Weather Prediction Analytics"
for d in find("dashboard", "dashboard_title", DASH):
    api("DELETE", f"dashboard/{d}")

# ---- database (key-pair auth, key mounted read-only at /app/keys) --------
DB_NAME = "Snowflake DEMO_DB"
db_ids = find("database", "database_name", DB_NAME)
db_payload = {
    "database_name": DB_NAME,
    "sqlalchemy_uri": f"snowflake://{args.user}@{args.account}/DEMO_DB/ANALYTICS?warehouse=DEMO_WH&role=ACCOUNTADMIN",
    "extra": json.dumps({"engine_params": {"connect_args": {"private_key_file": "/app/keys/rsa_key.p8"}}}),
    "expose_in_sqllab": True,
}
db_id = db_ids[0] if db_ids else api("POST", "database/", json=db_payload)["id"]
print("database", db_id)


def dataset(table):
    for ds in find("dataset", "table_name", table):
        for ch in api("GET", f"dataset/{ds}/related_objects")["charts"]["result"]:
            api("DELETE", f"chart/{ch['id']}")
        api("DELETE", f"dataset/{ds}")
    ds = api("POST", "dataset/", json={"database": db_id, "schema": "analytics", "table_name": table})["id"]
    api("PUT", f"dataset/{ds}/refresh")          # pull column metadata from Snowflake
    print("dataset", table, ds)
    return ds


metrics_ds = dataset("weather_metrics")
summary_ds = dataset("city_weather_summary")

# Snowflake reports unquoted identifiers in upper case, so physical columns are CITY, DATE, ...
TIME_FILTER = [{"clause": "WHERE", "subject": "DATE", "operator": "TEMPORAL_RANGE",
                "comparator": "No filter", "expressionType": "SIMPLE"}]


def m(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def timeseries(name, metrics, viz="echarts_timeseries_line", y_title="", extra=None):
    params = {
        "viz_type": viz, "datasource": f"{metrics_ds}__table",
        "x_axis": "DATE", "time_grain_sqla": "P1D", "time_range": "No filter",
        "metrics": metrics, "groupby": ["CITY"], "adhoc_filters": TIME_FILTER,
        "row_limit": 10000, "show_legend": True, "legendOrientation": "top",
        "rich_tooltip": True, "y_axis_title": y_title, "y_axis_title_position": "Left",
        "y_axis_title_margin": 30, "x_axis_time_format": "%b %d", "color_scheme": "supersetColors",
        **(extra or {}),
    }
    return name, metrics_ds, params


CHARTS = [
    timeseries("Temperature & Moving Averages (°C)",
               [m("MAX(temp_mean_c)", "Daily mean"), m("MAX(temp_ma_7d_c)", "7-day MA"),
                m("MAX(temp_ma_30d_c)", "30-day MA")], y_title="°C"),
    timeseries("Temperature Anomaly vs City Baseline (°C)",
               [m("MAX(temp_anomaly_c)", "Anomaly")], viz="echarts_timeseries_bar", y_title="°C"),
    timeseries("Rolling Rainfall (mm)",
               [m("MAX(rain_rolling_7d_mm)", "7-day rolling"), m("MAX(rain_rolling_30d_mm)", "30-day rolling")],
               y_title="mm"),
    timeseries("Dry Spell Length (consecutive dry days)",
               [m("MAX(dry_spell_length_days)", "Dry spell")], y_title="days",
               extra={"seriesType": "end"}),
    ("7-Day Forecast", metrics_ds, {
        "viz_type": "table", "datasource": f"{metrics_ds}__table", "query_mode": "raw",
        "all_columns": ["CITY", "DATE", "WEATHER_CATEGORY", "TEMP_MAX_C", "TEMP_MIN_C",
                        "PRECIPITATION_MM", "TEMP_ANOMALY_C"],
        "adhoc_filters": TIME_FILTER + [{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "IS_FORECAST"}],
        "order_by_cols": [json.dumps(["CITY", True]), json.dumps(["DATE", True])],
        "row_limit": 100, "time_range": "No filter", "include_search": False,
        "show_cell_bars": False, "table_timestamp_format": "%Y-%m-%d"}),
    ("City Summary (observed days)", summary_ds, {
        "viz_type": "table", "datasource": f"{summary_ds}__table", "query_mode": "raw",
        "all_columns": ["CITY", "FIRST_DATE", "LAST_DATE", "OBSERVED_DAYS", "AVG_TEMP_C", "RECORD_HIGH_C",
                        "RECORD_LOW_C", "TOTAL_PRECIPITATION_MM", "DRY_DAYS", "LONGEST_DRY_SPELL_DAYS"],
        "adhoc_filters": [], "row_limit": 10, "time_range": "No filter",
        "show_cell_bars": False, "table_timestamp_format": "%Y-%m-%d"}),
]

chart_ids = []
for name, ds, params in CHARTS:
    cid = api("POST", "chart/", json={"slice_name": name, "viz_type": params["viz_type"],
                                       "datasource_id": ds, "datasource_type": "table",
                                       "params": json.dumps(params)})["id"]
    chart_ids.append((cid, name))
    print("chart", cid, name)

# ---- dashboard layout: time-series charts 2 per row, then each table full width ----
pos = {"DASHBOARD_VERSION_KEY": "v2",
       "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
       "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
       "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASH}}}
ROWS = [[(0, 6, 55), (1, 6, 55)], [(2, 6, 55), (3, 6, 55)], [(4, 12, 62)], [(5, 12, 24)]]  # (chart, width, height)
for r, cells in enumerate(ROWS):
    row = f"ROW-{r}"
    pos["GRID_ID"]["children"].append(row)
    pos[row] = {"type": "ROW", "id": row, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    for i, width, height in cells:
        cid, name = chart_ids[i]
        key = f"CHART-{cid}"
        pos[row]["children"].append(key)
        pos[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                    "meta": {"chartId": cid, "width": width, "height": height, "sliceName": name}}

all_ids = [c for c, _ in chart_ids]
scope = {"rootPath": ["ROOT_ID"], "excluded": []}
native_filters = [
    {"id": "NATIVE_FILTER-city", "name": "City", "filterType": "filter_select", "type": "NATIVE_FILTER",
     "targets": [{"datasetId": metrics_ds, "column": {"name": "CITY"}}],
     "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "searchAllOptions": False,
                       "inverseSelection": False, "defaultToFirstItem": False},
     "defaultDataMask": {"filterState": {}, "extraFormData": {}}, "cascadeParentIds": [],
     "scope": scope, "chartsInScope": all_ids, "tabsInScope": [], "description": "Pick one or more cities"},
    {"id": "NATIVE_FILTER-date", "name": "Date Range", "filterType": "filter_time", "type": "NATIVE_FILTER",
     "targets": [{}], "controlValues": {},
     "defaultDataMask": {"filterState": {"value": "No filter"}, "extraFormData": {"time_range": "No filter"}},
     "cascadeParentIds": [], "scope": scope, "chartsInScope": all_ids, "tabsInScope": [],
     "description": "Restrict the time-series charts to a date range"},
]
dash = api("POST", "dashboard/", json={
    "dashboard_title": DASH, "slug": "weather-analytics", "published": True,
    "position_json": json.dumps(pos),
    "json_metadata": json.dumps({"native_filter_configuration": native_filters, "color_scheme": "supersetColors",
                                 "refresh_frequency": 0, "cross_filters_enabled": False,
                                 "chart_configuration": {}, "timed_refresh_immune_slices": []}),
})
for cid in all_ids:
    api("PUT", f"chart/{cid}", json={"dashboards": [dash["id"]]})
print(f"dashboard {dash['id']}: {args.url}/superset/dashboard/weather-analytics/")
