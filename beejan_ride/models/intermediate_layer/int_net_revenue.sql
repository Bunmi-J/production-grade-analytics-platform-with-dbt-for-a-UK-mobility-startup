-- this model calculates the net revenue for each trip by subtracting the payment fee from the total amount paid. It serves as an intermediate layer for further analysis on revenue performance and profitability of trips.

with payments as (
    select * from {{ ref('stg_payments_raw') }}
),

net_revenue as (
    select
        fee,
        amount,
        amount - fee as net_revenue_calc,
        trip_id,
        currency,
        created_at,
        payment_id,
        payment_status,
        payment_provider
    from payments

    
)

select * from net_revenue