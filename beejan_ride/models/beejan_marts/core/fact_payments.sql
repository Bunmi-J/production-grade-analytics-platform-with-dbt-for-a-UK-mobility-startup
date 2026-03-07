-- This model creates the fact_payments table which contains payment information for each trip, 
-- along with flags for failed payments, extreme surge pricing, and duplicate payments. It 
-- joins the raw payments data with the intermediate tables that flag failed payments and duplicates.
{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge',
    tags=['finance', 'fraud'],
    meta={
        'owner': 'beejan-finance-fraud-team',
        'email': 'bj2026@gmail.com'
    }

) }}

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
        f.rider_id,
        f.driver_id,
        p.amount,
        p.fee,
        p.payment_status,
       -- p.payment_method as payment_method_id,
        f.failed_payment_on_completed_trip_flag,
        f.extreme_surge_flag,
        d.duplicate_payment_flag,
        p.created_at
        
    from payments p
    left join failed f on p.trip_id = f.trip_id
    left join duplicates d on p.trip_id = d.trip_id
)

select * from final

{% if is_incremental() %}
where created_at > (select max(created_at) from {{ this }})
{% endif %}