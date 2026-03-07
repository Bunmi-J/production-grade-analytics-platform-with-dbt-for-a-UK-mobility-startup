-- This model creates the fact_trips table which contains detailed information about each trip,
-- including trip duration, net revenue, surge pricing, and various flags for failed payments, 
-- duplicates, and fraud indicators. It joins the raw trips data with intermediate tables that
-- calculate these metrics and flags.

{{ 
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        tags=['operations', 'finance', 'fraud'],
        meta={
        'owner': 'beejan-operations-team',
        'team': 'beejan-finance-fraud-team',
        'email': 'bj2026@gmail.com'
    }

    ) 
}}


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

driver_lt_trips as (
    select * from {{ ref('int_driver_lifetime_trips') }}
),  

final as (
    select
    -- primary key
        t.trip_id,
    -- foreign keys
        t.rider_id,
        t.driver_id,
        t.city_id,
        fp.payment_id,
      -- timestamps and durations  
        t.pickup_at,
        t.dropoff_at,
        t.requested_at,
        d.trip_duration_minutes,
        d.wait_time_minutes,
        -- metrics and flags
        t.actual_fare,
        t.estimated_fare,
        r.net_revenue_calc,
        t.surge_multiplier,
        fp.failed_payment_on_completed_trip_flag,
        fp.extreme_surge_flag, 
        dp.duplicate_payment_flag, 
        lt.driver_lifetime_trips,
        f.high_surge_flag, 
        f.suspicious_short_trip_flag, 
        f.suspicious_long_trip_flag, 
        f.zero_fare_flag, 
        f.missing_timestamps_flag,
    -- trip info
        t.created_at,
        t.updated_at,
        t.payment_method,
        t.is_corporate,
        t.status,
        d.is_completed,
        d.is_cancelled
    from trips t
    left join duration d on t.trip_id = d.trip_id
    left join revenue r on t.trip_id = r.trip_id
    left join failed_payments fp on t.trip_id = fp.trip_id
    left join duplicate_payments dp on t.trip_id = dp.trip_id
    left join fraud f on t.trip_id = f.trip_id
    left join driver_lt_trips lt on t.driver_id = lt.driver_id
)

select * from final

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
