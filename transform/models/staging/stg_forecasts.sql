select
    forecast_timestamp,
    station_id,
    wmo_station_id,
    temperature,
    precipitation,
    relative_humidity,
    wind_speed,
    wind_direction,
    condition,
    ingested_at
from {{ source('weather', 'weather_forecasts') }}
-- Only keep the latest forecast for each station at each timestamp
qualify row_number() over (partition by wmo_station_id, forecast_timestamp order by ingested_at desc) = 1
