with driver_status as (

    select *
    from {{ ref('stg_driver_status_eventsraw') }}

)

select
    event_id,
    driver_id,
    status,
    event_timestamp
from driver_status


 