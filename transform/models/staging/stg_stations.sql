select
    id as station_id,
    station_name,
    wmo_station_id,
    dwd_station_id,
    -- Avoid slight difference in geometry due to precision issue
    first_value(ST_Point(lon, lat)) over (partition by wmo_station_id order by id) as geometry
from {{ source('weather', 'weather_stations') }}
where wmo_station_id is not null
and last_record >= '2025-09-01'
-- Only 20 records have empty wmo_station_id, all are outdated with latest records as of 2023-05-03