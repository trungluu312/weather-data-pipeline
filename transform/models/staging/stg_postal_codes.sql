select
    plz as postal_code,
    geometry
from {{ source('weather', 'postal_codes') }}
