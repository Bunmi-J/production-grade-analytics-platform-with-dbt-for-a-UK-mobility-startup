with driver_status as (

    select *
    from {{ ref('stg_driver_status_events_raw') }}

)

select
    event_id,
    driver_id,
    status,
    event_timestamp
from driver_status


 