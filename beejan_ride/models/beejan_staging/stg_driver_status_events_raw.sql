{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}


with source as (
    select
        event_id,
        driver_id,
        status,
        event_timestamp
    from {{ source('beejan_dataset', 'driver_status_events_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (

    select
        event_id,
        driver_id,
        status,
        event_timestamp,
        {{ dedupe('event_id', 'event_timestamp') }}  --use macro for deduplication logic
    
    --    row_number() over (
    --        partition by event_id
    --       order by event_timestamp desc
    --   ) as rn
    from source
    where event_id is not null
      and driver_id is not null

),

final as (

    select
        cast(event_id as int64) as event_id,
        cast(driver_id as int64) as driver_id,
        cast(status as string) as status,
        cast(event_timestamp as timestamp) as event_timestamp
    from cleaned
    where rn = 1
 {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later on the same day as the last run of this model)
  and cast(event_timestamp as timestamp) > (select coalesce(max(event_timestamp), '1900-01-01') from {{ this }})

{% endif %}   

)

select * from final


