--

with payments as (
    select * from {{ ref('stg_payments_raw') }}
),

failed as (
    select * from {{ ref('int_failed_payment_surge_flag') }}
),

duplicates as (
    select * from {{ ref('int_duplicate_trip_payment') }}
),

final as (
    select
        p.payment_id,
        p.trip_id,
        p.rider_id,
        p.driver_id,
        p.payment_amount,
        p.payment_status,
        p.payment_method as payment_method_id,
        p.payment_timestamp,
        f.failed_payment_flag,
        d.duplicate_payment_flag,
        p.created_at,
        p.updated_at
    from payments p
    left join failed f on p.trip_id = f.trip_id
    left join duplicates d on p.trip_id = d.trip_id
)

select * from final