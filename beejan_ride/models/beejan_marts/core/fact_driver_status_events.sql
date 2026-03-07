-- This model creates the fact_driver_status_events table which captures all the status events
-- of drivers.
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    tags=['operations'],
    meta={
        'owner': 'beejan-operations-team',
        'email': 'bj2026@gmail.com'
    }

) }}

with driver_status as (
    select
        event_id,
        driver_id,
        status,
        event_timestamp
        
    from {{ ref('stg_driver_status_events_raw') }}
)

select * from driver_status

{% if is_incremental() %}
where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}