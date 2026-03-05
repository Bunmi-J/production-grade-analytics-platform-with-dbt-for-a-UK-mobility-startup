with trips as (
    select * from {{ ref('stg_trips_raw') }}
),

driver_trips as (
    select
        driver_id,
        trip_id
    from trips
    where status = 'completed'
),

lifetime_trips as (
    select
        driver_id,
        count(*) as driver_lifetime_trips
    from driver_trips
    group by driver_id
)

select * from lifetime_trips