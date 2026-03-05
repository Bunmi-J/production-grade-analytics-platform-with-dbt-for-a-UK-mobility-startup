with drivers as (

    select *
    from {{ ref('stg_drivers_raw') }}

)

select
    driver_id,
    city_id,
    vehicle_id,
    created_at,
    updated_at,
    driver_status,
    onboarding_date,
    rating
from drivers


 