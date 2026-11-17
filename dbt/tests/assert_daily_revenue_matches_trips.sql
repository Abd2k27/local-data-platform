-- The daily mart must reconcile with the fact table: any returned row is a failure.
with fact as (
    select pickup_date, sum(total_amount) as revenue, count(*) as trips
    from {{ ref('fct_trips') }}
    group by pickup_date
)

select m.pickup_date
from {{ ref('mart_daily_revenue') }} as m
join fact as f using (pickup_date)
where m.revenue <> f.revenue
   or m.trips <> f.trips
