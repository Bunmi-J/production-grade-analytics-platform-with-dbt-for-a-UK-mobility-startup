with source as (
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
        surge_multiplier
    from {{ source('beejan_dataset', 'trips_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (
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
        row_number() over (
            partition by trip_id
            order by updated_at desc nulls last, created_at desc
        ) as rn
    from source
    where trip_id is not null
      and rider_id is not null
      and driver_id is not null
),

final as (

    select
        cast(trip_id as int64) as trip_id,
        cast(rider_id as int64) as rider_id,
        cast(driver_id as int64) as driver_id,
        cast(city_id as int64) as city_id,
        cast(status as string) as status,
        cast(payment_method as string) as payment_method,
        cast(is_corporate as bool) as is_corporate,

        cast(actual_fare as float64) as actual_fare,
        cast(estimated_fare as float64) as estimated_fare,
        cast(surge_multiplier as float64) as surge_multiplier,

        cast(requested_at as timestamp) as requested_at,
        cast(pickup_at as timestamp) as pickup_at,
        cast(dropoff_at as timestamp) as dropoff_at,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from cleaned
    where rn = 1

)

select * from final

