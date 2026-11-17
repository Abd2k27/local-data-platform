{# Materialised as a table (not a view): millions of rows are re-read by every
   downstream test and model, so computing them once is much faster. #}
{{ config(materialized='table') }}

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        md5(source_file || '-' || row_num::text)            as trip_id,
        vendorid                                             as vendor_id,
        tpep_pickup_datetime                                 as pickup_at,
        tpep_dropoff_datetime                                as dropoff_at,
        passenger_count::integer                             as passenger_count,
        trip_distance::numeric(10, 2)                        as trip_distance_miles,
        ratecodeid::integer                                  as rate_code_id,
        store_and_fwd_flag = 'Y'                             as is_store_and_forward,
        pulocationid                                         as pickup_zone_id,
        dolocationid                                         as dropoff_zone_id,
        payment_type                                         as payment_type_id,
        fare_amount::numeric(10, 2)                          as fare_amount,
        extra::numeric(10, 2)                                as extra_amount,
        mta_tax::numeric(10, 2)                              as mta_tax_amount,
        tip_amount::numeric(10, 2)                           as tip_amount,
        tolls_amount::numeric(10, 2)                         as tolls_amount,
        improvement_surcharge::numeric(10, 2)                as improvement_surcharge,
        coalesce(congestion_surcharge, 0)::numeric(10, 2)    as congestion_surcharge,
        coalesce(airport_fee, 0)::numeric(10, 2)             as airport_fee,
        coalesce(cbd_congestion_fee, 0)::numeric(10, 2)      as cbd_congestion_fee,
        total_amount::numeric(10, 2)                         as total_amount,
        source_file,
        -- The month the file is supposed to cover, e.g. yellow_tripdata_2024-01.parquet
        to_date(substring(source_file from '(\d{4}-\d{2})'), 'YYYY-MM') as source_month
    from source

)

select
    *,
    round(extract(epoch from (dropoff_at - pickup_at)) / 60.0, 2) as trip_duration_minutes
from renamed
