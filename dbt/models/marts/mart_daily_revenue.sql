select
    pickup_date,
    count(*)                                   as trips,
    sum(total_amount)                          as revenue,
    round(avg(total_amount), 2)                as avg_ticket,
    round(avg(trip_distance_miles), 2)         as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)       as avg_duration_minutes,
    round(avg(tip_ratio), 4)                   as avg_tip_ratio,
    round(100.0 * avg((payment_type_id = 1)::int), 2) as card_payment_pct
from {{ ref('fct_trips') }}
group by pickup_date
