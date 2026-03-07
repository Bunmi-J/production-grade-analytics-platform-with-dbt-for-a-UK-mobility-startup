-- This SQL file is designed to identify any trips in the `fact_trips` table that have a 
-- non-positive trip duration, which could indicate potential data quality issues. 
--The query selects all records from the `fact_trips` table where the `trip_duration_minutes` 
--column has a value less than or equal to zero.

select *
from {{ ref('fact_trips') }}
where trip_duration_minutes <= 0