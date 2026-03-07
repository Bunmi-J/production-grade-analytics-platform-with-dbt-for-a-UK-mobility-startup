-- This model creates a dimension table for riders, which includes rider_id, country, referral_code
-- signup_date, and created_at. It serves as a reference for other models that need to join with 
--rider information, such as trips or payments. The data is sourced from the stg_riders_raw staging table,
-- which is expected to have been cleaned and deduplicated in the staging layer.
{{ config(
    materialized='table',
    tags=['operations'],
    meta={
        'owner': 'beejan-operations-team',
        'email': 'bj2026@gmail.com'
    }
) }}
with riders as (

    select *
    from {{ ref('stg_riders_raw') }}

)

select
    rider_id,
    country,
    referral_code,
    signup_date,
    created_at
from riders
