with trips as (
    select *
    from {{ ref('stg_trips_raw') }}
    where status = 'completed'
),

rider_ltv as (
    select
        rider_id,
        count(*) as lifetime_trips,
        sum(actual_fare) as rider_lifetime_value,
        avg(actual_fare) as avg_trip_value
    from trips
    group by rider_id
)

select * from rider_ltv