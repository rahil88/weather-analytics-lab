-- Cleaned, typed daily weather: one row per city per day, with a surrogate key
-- and a readable weather category from the WMO weather code.
with source as (
    select * from {{ source('raw', 'weather_daily') }}
)

select
    city || '|' || to_varchar(date, 'YYYY-MM-DD')   as weather_id,
    city,
    latitude,
    longitude,
    date,
    temp_max                                        as temp_max_c,
    temp_min                                        as temp_min_c,
    coalesce(temp_mean, (temp_max + temp_min) / 2)  as temp_mean_c,
    temp_max - temp_min                             as temp_range_c,
    coalesce(precipitation_sum, 0)                  as precipitation_mm,
    coalesce(rain_sum, 0)                           as rain_mm,
    wind_speed_max                                  as wind_speed_max_kmh,
    weather_code,
    case
        when weather_code = 0                  then 'Clear'
        when weather_code between 1 and 3      then 'Cloudy'
        when weather_code in (45, 48)          then 'Fog'
        when weather_code between 51 and 57    then 'Drizzle'
        when weather_code between 61 and 67    then 'Rain'
        when weather_code between 71 and 77    then 'Snow'
        when weather_code between 80 and 82    then 'Rain Showers'
        when weather_code in (85, 86)          then 'Snow Showers'
        when weather_code between 95 and 99    then 'Thunderstorm'
        else 'Unknown'
    end                                             as weather_category,
    is_forecast,
    loaded_at
from source
