{#
  Business-grade trips: we drop records that are physically impossible or
  outside the month their file is supposed to cover (TLC files contain stray dates).
#}
with trips as (

    select * from {{ ref('stg_yellow_trips') }}

)

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_at::date                                   as pickup_date,
    extract(hour from pickup_at)::integer             as pickup_hour,
    extract(isodow from pickup_at)::integer           as pickup_day_of_week,
    pickup_zone_id,
    dropoff_zone_id,
    coalesce(payment_type_id, 5)                      as payment_type_id,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    case when fare_amount > 0 then round(tip_amount / fare_amount, 4) end as tip_ratio,
    case when trip_duration_minutes > 0
         then round(trip_distance_miles / (trip_duration_minutes / 60.0), 2)
    end                                               as avg_speed_mph,
    source_month
from trips
where pickup_at >= source_month
  and pickup_at <  source_month + interval '1 month'
  and dropoff_at > pickup_at
  and trip_duration_minutes <= 6 * 60
  and trip_distance_miles > 0
  and trip_distance_miles < 200
  and fare_amount >= 0
  and total_amount >= 0
