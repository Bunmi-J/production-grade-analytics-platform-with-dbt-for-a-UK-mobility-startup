-- this model calculates the trip duration and wait time for each trip, as well as flags for completed and cancelled trips. It serves as an intermediate layer for further analysis on trip performance and driver/rider behavior.
-- It also includes a corporate_trip_flag to identify trips that were booked as corporate rides, which may have different characteristics and performance metrics compared to regular trips.

with trips as (
    select * from {{ ref('stg_trips_raw') }}
),

duration as (
    select
        status,
        city_id,
        trip_id,
        rider_id,
        driver_id,
        pickup_at,
        dropoff_at,
        created_at,
        updated_at,
        actual_fare,
        is_corporate,
        requested_at,
        estimated_fare,
        payment_method,
        surge_multiplier,
        -- reusable transformation logic
        timestamp_diff(dropoff_at, pickup_at, minute) as trip_duration_minutes,
        timestamp_diff(pickup_at, requested_at, minute) as wait_time_minutes,

        case when is_corporate = true then 1 else 0 end as corporate_trip_flag,
        case when status = 'completed' then 1 else 0 end as is_completed,
        case when status = 'cancelled' then 1 else 0 end as is_cancelled
    from trips
)

select * from duration