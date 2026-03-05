with source as (
    select
        country,
        rider_id,
        created_at,
        signup_date,
        referral_code
    from {{ source('beejan_dataset', 'riders_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (
    select
        country,
        rider_id,
        created_at,
        signup_date,
        referral_code,
        {{ dedupe('rider_id', 'created_at') }}   --use macro for deduplication logic

         --row_number() over (
           -- partition by rider_id
            --order by created_at desc
        --) as rn
    from source
    where rider_id is not null

),

final as (

    select
        cast(rider_id as int64) as rider_id,
        cast(country as string) as country,
        cast(referral_code as string) as referral_code,
        cast(created_at as timestamp) as created_at,
        cast(signup_date as date) as signup_date
    from cleaned
    where rn = 1

)

select * from final