select
    z.borough,
    z.zone_name,
    t.pickup_zone_id,
    t.pickup_day_of_week,
    t.pickup_hour,
    count(*)                                   as trips,
    round(avg(t.trip_duration_minutes), 2)     as avg_duration_minutes,
    round(avg(t.total_amount), 2)              as avg_ticket,
    round(avg(t.avg_speed_mph), 2)             as avg_speed_mph
from {{ ref('fct_trips') }} as t
left join {{ ref('dim_zones') }} as z
    on t.pickup_zone_id = z.zone_id
group by 1, 2, 3, 4, 5
