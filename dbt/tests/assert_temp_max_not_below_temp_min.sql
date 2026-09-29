-- Fails if any day reports a max temperature lower than its min temperature.
select weather_id, temp_max_c, temp_min_c
from {{ ref('stg_weather_daily') }}
where temp_max_c < temp_min_c
