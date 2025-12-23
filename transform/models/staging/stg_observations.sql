select
    timestamp,
    station_id,
    wmo_station_id,
    temperature,
    precipitation,
    relative_humidity,
    wind_speed,
    wind_direction,
    ingested_at
from {{ source('weather', 'weather_observations') }}
-- Only keep the latest observation for each station at each timestamp
qualify row_number() over (partition by wmo_station_id, timestamp order by ingested_at desc) = 1
