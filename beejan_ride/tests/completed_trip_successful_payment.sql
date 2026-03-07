-- This SQL file is designed to identify any completed trips in the `fact_trips` table that do not
-- have a corresponding successful payment in the `fact_payments` table. 
-- The query first creates a Common Table Expression (CTE) named `completed_trips`
with completed_trips as (
    select trip_id
    from {{ ref('fact_trips') }}
    where dropoff_at is not null
),

successful_payments as (
    select distinct trip_id
    from {{ ref('fact_payments') }}
    where payment_status = 'success'
)

select ct.trip_id
from completed_trips ct
left join successful_payments sp
    on ct.trip_id = sp.trip_id
where sp.trip_id is null