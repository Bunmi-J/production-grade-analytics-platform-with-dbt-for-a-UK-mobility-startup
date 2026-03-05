with source as (
    select 
        city_id,
        country,
        city_name,
        launch_date
    from {{ source('beejan_dataset', 'cities_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (

    select
        city_id,
        country,
        city_name,
        launch_date,
        {{ dedupe('city_id', 'launch_date') }}  -- use macro for deduplication logic

       -- row_number() over (
       --     partition by city_id
       --     order by launch_date desc
        --) as rn
    from source
    where city_id is not null

),

-- cast data types and standardize timestamps
final as (

    select
        cast(city_id as int64) as city_id,
        cast(country as string) as country,
        cast(city_name as string) as city_name,
        cast(launch_date as timestamp) as launch_date
    from cleaned
    where rn = 1

)
select * from final

