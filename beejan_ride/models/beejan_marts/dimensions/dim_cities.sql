-- This model creates a dimension table for cities, which includes city_id, city_name, country, and launch_date. It serves as a reference for other models that need to join with city information, such as trips or drivers. The data is sourced from the stg_cities_raw staging table, which is expected to have been cleaned and deduplicated in the staging layer.

with cities as (

    select *
    from {{ ref('stg_cities_raw') }}

)

select
    city_id,
    city_name,
    country
    launch_date
from cities

 