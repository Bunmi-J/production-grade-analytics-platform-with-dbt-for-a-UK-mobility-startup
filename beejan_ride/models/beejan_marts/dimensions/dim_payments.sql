-- This model creates a dimension table for payments, which includes payment_id, trip_id, amount, fee, currency, payment_provider, payment_status, and created_at. It serves as a reference for other models that need to join with payment information, such as trips or riders. The data is sourced from the stg_payments_raw staging table, which is expected to have been cleaned and deduplicated in the staging layer.

with payments as (

    select *
    from {{ ref('stg_payments_raw') }}

)

select
    payment_id,
    trip_id,
    amount,
    fee,
    currency,
    payment_provider,
    payment_status,
    created_at
from payments
