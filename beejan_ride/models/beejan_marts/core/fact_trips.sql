-- 

with trips as (
    select * from {{ ref('stg_trips_raw') }}
),

duration as (
    select * from {{ ref('int_trip_duration') }}
),

revenue as (
    select * from {{ ref('int_net_revenue') }}
),

failed_payments as (
    select * from {{ ref('int_failed_payment_surge_flag') }}
),

duplicate_payments as (
    select * from {{ ref('int_duplicate_trip_payment') }}
),

fraud as (
    select * from {{ ref('int_fraud_indicators') }}
),

final as (
    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.city_id,
        t.trip_start_timestamp,
        t.trip_end_timestamp,
        d.trip_duration_minutes,
        t.distance_km,
        t.surge_multiplier,
        r.net_revenue,
        fp.failed_payment_flag,
        dp.duplicate_payment_flag,
        f.fraud_flag,
        f.fraud_score,
        t.created_at,
        t.updated_at
    from trips t
    left join duration d on t.trip_id = d.trip_id
    left join revenue r on t.trip_id = r.trip_id
    left join failed_payments fp on t.trip_id = fp.trip_id
    left join duplicates dp on t.trip_id = dp.trip_id
    left join fraud f on t.trip_id = f.trip_id
)

select * from final