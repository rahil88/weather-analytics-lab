-- Daily weather metrics per city:
--   * 7-day and 30-day moving averages of mean temperature
--   * temperature anomaly vs. the city's observed baseline (difference and z-score)
--   * 7-day and 30-day rolling rainfall
--   * dry-spell length: number of consecutive dry days ending on this date
with daily as (
    select * from {{ ref('stg_weather_daily') }}
),

baseline as (
    -- Baseline uses observed days only, so forecasts do not shift it
    select
        city,
        avg(temp_mean_c)    as baseline_temp_c,
        stddev(temp_mean_c) as baseline_temp_stddev_c
    from daily
    where not is_forecast
    group by city
),

flagged as (
    select
        d.*,
        d.precipitation_mm < {{ var('dry_day_threshold_mm') }} as is_dry_day,
        -- Each wet day starts a new group, so consecutive dry days share one group id
        sum(iff(d.precipitation_mm < {{ var('dry_day_threshold_mm') }}, 0, 1))
            over (partition by d.city order by d.date rows between unbounded preceding and current row)
            as wet_day_group
    from daily d
)

select
    f.weather_id,
    f.city,
    f.date,
    f.is_forecast,
    f.weather_category,
    f.temp_max_c,
    f.temp_min_c,
    f.temp_mean_c,
    f.temp_range_c,
    f.precipitation_mm,
    f.wind_speed_max_kmh,

    round(avg(f.temp_mean_c) over (partition by f.city order by f.date rows between 6 preceding and current row), 2)
        as temp_ma_7d_c,
    round(avg(f.temp_mean_c) over (partition by f.city order by f.date rows between 29 preceding and current row), 2)
        as temp_ma_30d_c,

    round(b.baseline_temp_c, 2) as baseline_temp_c,
    round(f.temp_mean_c - b.baseline_temp_c, 2) as temp_anomaly_c,
    round((f.temp_mean_c - b.baseline_temp_c) / nullif(b.baseline_temp_stddev_c, 0), 2) as temp_anomaly_zscore,

    round(sum(f.precipitation_mm) over (partition by f.city order by f.date rows between 6 preceding and current row), 1)
        as rain_rolling_7d_mm,
    round(sum(f.precipitation_mm) over (partition by f.city order by f.date rows between 29 preceding and current row), 1)
        as rain_rolling_30d_mm,

    f.is_dry_day,
    iff(
        f.is_dry_day,
        sum(iff(f.is_dry_day, 1, 0)) over (
            partition by f.city, f.wet_day_group order by f.date
            rows between unbounded preceding and current row
        ),
        0
    ) as dry_spell_length_days
from flagged f
join baseline b on f.city = b.city
