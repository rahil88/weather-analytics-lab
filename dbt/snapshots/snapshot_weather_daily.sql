-- SCD Type 2 history of the daily weather. Open-Meteo revises recent days and
-- forecasts on every run; this snapshot keeps each version (dbt_valid_from / dbt_valid_to),
-- so we can later compare what was forecast with what was observed.
{% snapshot snapshot_weather_daily %}

{{
    config(
      target_schema='SNAPSHOT',
      unique_key='weather_id',
      strategy='check',
      check_cols=['temp_max_c', 'temp_min_c', 'temp_mean_c', 'precipitation_mm', 'weather_code', 'is_forecast'],
    )
}}

select * from {{ ref('stg_weather_daily') }}

{% endsnapshot %}
