-- This model creates the fact_driver_status_events table which captures all the status events
-- of drivers.
with driver_status as (
    select
        event_id,
        driver_id,
        status,
        event_timestamp
        
    from {{ ref('stg_driver_status_events_raw') }}
)

select * from driver_status