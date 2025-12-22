with stations as (
    select * from {{ ref('stg_stations') }}
    where wmo_station_id is not null
),
postal_codes as (
    select * from {{ ref('stg_postal_codes') }}
),
valid_stations_with_temp as (
    select distinct wmo_station_id from {{ ref('stg_observations') }} where temperature is not null
    union
    select distinct wmo_station_id from {{ ref('stg_forecasts') }} where temperature is not null
),
distances as (
    select distinct
        s.wmo_station_id,
        p.postal_code,
        ST_Distance_Sphere(s.geometry, ST_Centroid(p.geometry)) as dist
    from stations s
    inner join valid_stations_with_temp v on s.wmo_station_id = v.wmo_station_id
    cross join postal_codes p
    where ST_Distance_Sphere(s.geometry, ST_Centroid(p.geometry)) <= 15000 
    -- Max search radius 15km for nearby stations in case of missing data or no stations in the area
),
ranked as (
    select
        wmo_station_id,
        postal_code,
        dist,
        dense_rank() over (partition by postal_code order by dist asc) as station_rank
    from distances
)

select * 
from ranked
where station_rank <= 10 -- Limit to 10 nearest stations
