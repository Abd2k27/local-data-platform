select
    zone_id,
    borough,
    zone_name,
    service_zone,
    borough in ('Manhattan')                  as is_manhattan,
    zone_name ilike '%airport%'               as is_airport
from {{ ref('stg_taxi_zones') }}
