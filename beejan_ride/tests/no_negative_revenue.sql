-- This SQL file is designed to identify any trips in the `fact_trips` table that have a negative 
--net revenue, which could indicate potential data quality issues or fraudulent activity. 
-- The query selects all records from the `fact_trips` table where the `net_revenue` column has a 
--value less than zero.
select *
from {{ ref('fact_trips') }}
where net_revenue_calc < 0