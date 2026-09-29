"""
Weather ETL: Open-Meteo API -> Snowflake RAW.WEATHER_DAILY

- Cities, history window and forecast window come from Airflow Variables.
- Snowflake credentials come from the Airflow Connection "snowflake_conn".
- The load runs in one SQL transaction (BEGIN / COMMIT, ROLLBACK on error), and
  it deletes and re-inserts the fetched window, so re-running the DAG is idempotent.
- On success, the load task updates the WEATHER_DAILY Dataset. That update
  triggers the dbt DAG (weather_dbt.py).
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

API_URL = "https://api.open-meteo.com/v1/forecast"
TARGET_TABLE = "DEMO_DB.RAW.WEATHER_DAILY"
WEATHER_DAILY_DATASET = Dataset("snowflake://DEMO_DB/RAW/WEATHER_DAILY")

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "wind_speed_10m_max",
    "weather_code",
]


def return_snowflake_conn():
    # Credentials live in the Airflow Connection "snowflake_conn", never in code
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn().cursor()


@task
def get_cities():
    """Airflow Variable `weather_cities`: JSON list of {city, latitude, longitude, timezone}."""
    cities = Variable.get("weather_cities", deserialize_json=True)
    if len(cities) < 2:
        raise ValueError("weather_cities must contain at least two cities")
    return cities


@task
def extract(city):
    """Call Open-Meteo for one city: `past_days` of history plus `forecast_days` of forecast."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "daily": DAILY_FIELDS,
        "past_days": int(Variable.get("weather_past_days", default_var=60)),
        "forecast_days": int(Variable.get("weather_forecast_days", default_var=7)),
        "timezone": city["timezone"],
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return {"city": city, "payload": response.json()}


@task
def transform(extracted):
    """Flatten the API's column arrays into rows (JSON-serializable for XCom)."""
    city = extracted["city"]
    daily = extracted["payload"]["daily"]
    today = datetime.now(ZoneInfo(city["timezone"])).date().isoformat()

    records, skipped = [], 0
    for i, day in enumerate(daily["time"]):
        # Days older than the API's archive window come back all-null: skip them
        if daily["temperature_2m_max"][i] is None or daily["temperature_2m_min"][i] is None:
            skipped += 1
            continue
        code = daily["weather_code"][i]
        records.append([
            city["city"],
            city["latitude"],
            city["longitude"],
            day,                                   # 'YYYY-MM-DD'
            daily["temperature_2m_max"][i],
            daily["temperature_2m_min"][i],
            daily["temperature_2m_mean"][i],
            daily["precipitation_sum"][i],
            daily["rain_sum"][i],
            daily["wind_speed_10m_max"][i],
            int(code) if code is not None else None,
            day >= today,                          # today onward is still a forecast
        ])
    if not records:
        raise ValueError(f"No usable rows returned for {city['city']}")
    print(f"{city['city']}: {len(records)} rows ({records[0][3]} ~ {records[-1][3]}), skipped {skipped} empty days")
    # The delete window is the full requested range, so stale rows inside it are replaced too
    return {"city": city["city"], "start": daily["time"][0], "end": daily["time"][-1], "rows": records}


@task(outlets=[WEATHER_DAILY_DATASET])
def load(city_batches, target_table):
    """Idempotent load: delete the fetched window per city, then insert, in one transaction."""
    cur = return_snowflake_conn()

    # DDL auto-commits in Snowflake, so create schema/table BEFORE the transaction
    cur.execute("CREATE SCHEMA IF NOT EXISTS DEMO_DB.RAW")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            city              VARCHAR(50)   NOT NULL,
            latitude          NUMBER(9, 6)  NOT NULL,
            longitude         NUMBER(9, 6)  NOT NULL,
            date              DATE          NOT NULL,
            temp_max          FLOAT,
            temp_min          FLOAT,
            temp_mean         FLOAT,
            precipitation_sum FLOAT,
            rain_sum          FLOAT,
            wind_speed_max    FLOAT,
            weather_code      INTEGER,
            is_forecast       BOOLEAN       NOT NULL,
            loaded_at         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (city, date)
        )
    """)

    insert_sql = f"""
        INSERT INTO {target_table}
            (city, latitude, longitude, date, temp_max, temp_min, temp_mean,
             precipitation_sum, rain_sum, wind_speed_max, weather_code, is_forecast)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cur.execute("BEGIN;")
        total = 0
        for batch in city_batches:             # one batch per city (mapped task output)
            city, start, end, records = batch["city"], batch["start"], batch["end"], batch["rows"]
            cur.execute(
                f"DELETE FROM {target_table} WHERE city = %s AND date BETWEEN %s AND %s",
                (city, start, end),
            )
            cur.executemany(insert_sql, records)
            total += len(records)
            print(f"Staged {len(records)} rows for {city} ({start} ~ {end})")
        cur.execute("COMMIT;")
        print(f"Committed {total} rows into {target_table}")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Load failed, transaction rolled back: {e}")
        raise
    finally:
        cur.close()


@task
def check_table_stats(target_table):
    """Log per-city row counts and date ranges so every run shows what was loaded."""
    cur = return_snowflake_conn()
    try:
        cur.execute(f"""
            SELECT city, COUNT(*), MIN(date), MAX(date), SUM(IFF(is_forecast, 1, 0))
            FROM {target_table} GROUP BY city ORDER BY city
        """)
        for city, count, min_date, max_date, forecast_rows in cur.fetchall():
            print(f"{city}: {count} rows, {min_date} ~ {max_date}, {forecast_rows} forecast days")
    finally:
        cur.close()


with DAG(
    dag_id="weather_etl",
    description="Open-Meteo daily weather for multiple cities -> Snowflake RAW.WEATHER_DAILY",
    start_date=datetime(2026, 9, 28),
    schedule="30 2 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
    tags=["ETL", "weather"],
) as dag:
    cities = get_cities()
    raw = extract.expand(city=cities)          # one extract task per city
    rows = transform.expand(extracted=raw)     # one transform task per city
    loaded = load(rows, TARGET_TABLE)
    loaded >> check_table_stats(TARGET_TABLE)
