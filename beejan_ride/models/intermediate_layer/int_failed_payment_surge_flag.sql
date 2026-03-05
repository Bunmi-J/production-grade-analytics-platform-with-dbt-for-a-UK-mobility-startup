-- This model identifies completed trips that have failed payments, which may indicate potential issues with the payment processing or fraudulent activities. It serves as an intermediate layer for further analysis on payment failures and risk assessment of trips.
-- It also includes an extreme surge flag to identify trips that had a surge multiplier above a certain threshold, which may be associated with higher risk of payment failures or customer dissatisfaction.
with trips as (
    select *
    from {{ ref('int_trip_duration') }}   -- already includes duration, wait time, corporate flag
),

payments as (
    select *
    from {{ ref('stg_payments_raw') }}
),

trip_payments as (
    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.status,
        t.actual_fare,
        t.surge_multiplier,
        t.pickup_at,
        t.dropoff_at,

        p.payment_id,
        p.payment_status,
        p.amount,
        p.fee,

        case
            when t.status = 'completed'
             and (p.payment_status is null or p.payment_status != 'success')
            then 1 else 0
        end as failed_payment_on_completed_trip_flag,
        -- extreme surge multiplier
        case
            when t.surge_multiplier > 10 then 1
            else 0
        end as extreme_surge_flag
    from trips t
    left join payments p
        on t.trip_id = p.trip_id
    
)

select * from trip_payments
