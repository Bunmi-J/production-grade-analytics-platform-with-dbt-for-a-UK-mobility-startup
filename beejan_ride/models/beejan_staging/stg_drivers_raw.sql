with source as (
    select
        rating,
        driver_id,
        city_id,
        created_at,
        updated_at,
        vehicle_id,
        driver_status,
        onboarding_date
    from {{ source('beejan_dataset', 'drivers_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (
    select
        rating,
        driver_id,
        city_id,
        created_at,
        updated_at,
        vehicle_id,
        driver_status,
        onboarding_date,
         {{ dedupe('driver_id', 'updated_at') }}

         --row_number() over (
           -- partition by driver_id
            --order by updated_at desc
        --) as rn
    
    from source
    where driver_id is not null
      and vehicle_id is not null

),

final as (

    select
        cast(driver_id as int64) as driver_id,
        cast(city_id as int64) as city_id,
        cast(vehicle_id as string) as vehicle_id,
        cast(driver_status as string) as driver_status,
        cast(rating as float64) as rating,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(onboarding_date as date) as onboarding_date
    from cleaned
    where rn = 1

)

select * from final
