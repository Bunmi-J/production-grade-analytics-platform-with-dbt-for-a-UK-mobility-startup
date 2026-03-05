-- this model calculates various fraud indicators for each trip based on the trip duration, surge multiplier, payment method, and other factors. It serves as an intermediate layer for further analysis on potential fraudulent activities and risk assessment of trips.

with trips as (
    select * from {{ ref('int_trip_duration') }}
),

fraud as (
    select
        trip_id,
        rider_id,
        driver_id,
        city_id,
        actual_fare,
        surge_multiplier,
        trip_duration_minutes,
        wait_time_minutes,
        payment_method,
        status,

        -- fraud indicators
        case when surge_multiplier > 3 then 1 else 0 end as high_surge_flag,
        case when trip_duration_minutes < 1 then 1 else 0 end as suspicious_short_trip_flag,
        case when trip_duration_minutes > 180 then 1 else 0 end as suspicious_long_trip_flag,
        case when actual_fare = 0 and status = 'completed' then 1 else 0 end as zero_fare_flag,
        case when pickup_at is null or dropoff_at is null then 1 else 0 end as missing_timestamps_flag
    from trips
)

select * from fraud