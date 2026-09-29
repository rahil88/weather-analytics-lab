-- Fails if a dry spell is inconsistent: wet days must have length 0, dry days length >= 1.
select weather_id, is_dry_day, dry_spell_length_days
from {{ ref('weather_metrics') }}
where (is_dry_day and dry_spell_length_days < 1)
   or (not is_dry_day and dry_spell_length_days <> 0)
