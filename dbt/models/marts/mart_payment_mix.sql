select
    date_trunc('month', t.pickup_date)::date   as month,
    p.payment_type,
    v.vendor_name,
    count(*)                                   as trips,
    sum(t.total_amount)                        as revenue,
    round(avg(t.tip_ratio), 4)                 as avg_tip_ratio
from {{ ref('fct_trips') }} as t
left join {{ ref('payment_types') }} as p
    on t.payment_type_id = p.payment_type_id
left join {{ ref('vendors') }} as v
    on t.vendor_id = v.vendor_id
group by 1, 2, 3
