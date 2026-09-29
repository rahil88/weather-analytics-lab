-- One row per city: headline numbers for the dashboard's KPI tiles (observed days only).
select
    city,
    min(date)                                   as first_date,
    max(date)                                   as last_date,
    count(*)                                    as observed_days,
    round(avg(temp_mean_c), 2)                  as avg_temp_c,
    max(temp_max_c)                             as record_high_c,
    min(temp_min_c)                             as record_low_c,
    round(sum(precipitation_mm), 1)             as total_precipitation_mm,
    sum(iff(is_dry_day, 1, 0))                  as dry_days,
    max(dry_spell_length_days)                  as longest_dry_spell_days,
    round(max(abs(temp_anomaly_c)), 2)          as max_abs_temp_anomaly_c
from {{ ref('weather_metrics') }}
where not is_forecast
group by city
