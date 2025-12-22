with source as (
    select * from {{ ref('stg_observations') }}
),
station_map as (
    select * from {{ ref('int_station_location_map') }}
)

select distinct
    s.wmo_station_id,
    m.postal_code,
    s.timestamp,
    date_trunc('hour', s.timestamp) as observation_hour,
    s.temperature,
    s.precipitation,
    s.relative_humidity,
    s.wind_speed
from source s
left join station_map m on s.wmo_station_id = m.wmo_station_id
where 
    -- Cleaning Rules (allow NULLs to pass through)
    (s.temperature between -50 and 60 or s.temperature is null)
    and (s.relative_humidity between 0 and 100 or s.relative_humidity is null)
    and m.postal_code is not null -- Filter out stations not in target area
