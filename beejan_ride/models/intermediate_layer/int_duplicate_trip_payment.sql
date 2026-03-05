-- This model identifies trips that have duplicate payment records, which may indicate potential issues with the payment processing or fraudulent activities. It serves as an intermediate layer for further analysis on payment anomalies and risk assessment of trips.

with payments as (
    select * from {{ ref('stg_payments_raw') }}
),

duplicates as (
    select
        trip_id,
        count(*) as payment_count,
        countif(payment_status = 'success') as successful_payments,
        array_agg(payment_id) as payment_ids
    from payments
    group by trip_id
),

duplicate_flagged as (
    select
        trip_id,
        payment_count,
        successful_payments,
        payment_ids,
        case when successful_payments > 1 then 1 else 0 end as duplicate_payment_flag
    from duplicates
)

select * from duplicate_flagged