with time_spine as (
    select distinct date_trunc('hour', timestamp) as observation_hour
    from {{ ref('int_cleaned_observations') }}
),
pc_spine as (
    select p.postal_code, t.observation_hour
    from (select distinct postal_code from {{ ref('stg_postal_codes') }}) p
    cross join time_spine t
),
ranked_data as (
    select
        p.postal_code,
        p.observation_hour,
        c.temperature,
        c.precipitation,
        c.relative_humidity,
        c.wind_speed,
        m.station_rank
    from pc_spine p
    inner join {{ ref('int_station_location_map') }} m on p.postal_code = m.postal_code
    left join {{ ref('int_cleaned_observations') }} c 
        on m.wmo_station_id = c.wmo_station_id 
        and p.postal_code = c.postal_code
        and p.observation_hour = c.observation_hour
),
best_spatial as (
    select *
    from ranked_data
    where temperature is not null
    qualify row_number() over (partition by postal_code, observation_hour order by station_rank) = 1
),
merged_with_spine as (
    select
        p.postal_code,
        p.observation_hour,
        b.temperature,
        b.precipitation,
        b.relative_humidity,
        b.wind_speed
    from pc_spine p
    left join best_spatial b 
        on p.postal_code = b.postal_code 
        and p.observation_hour = b.observation_hour
)

select
    postal_code,
    observation_hour,
    
    -- Final metrics with temporal fallback (LAG)
    coalesce(temperature, lag(temperature) over (partition by postal_code order by observation_hour)) as temperature,
    coalesce(precipitation, lag(precipitation) over (partition by postal_code order by observation_hour)) as precipitation,
    coalesce(relative_humidity, lag(relative_humidity) over (partition by postal_code order by observation_hour)) as relative_humidity,
    coalesce(wind_speed, lag(wind_speed) over (partition by postal_code order by observation_hour)) as wind_speed

from merged_with_spine
order by 1, 2
