with source as (
    select
        fee,
        amount,
        trip_id,
        currency,
        created_at,
        payment_id,
        payment_status,
        payment_provider
    from {{ source('beejan_dataset', 'payments_raw') }}
),

-- remove invalid primary keys and dedupe
cleaned as (
    select
        fee,
        amount,
        trip_id,
        currency,
        created_at,
        payment_id,
        payment_status,
        payment_provider,
        {{ dedupe('payment_id', 'created_at') }}
        --row_number() over (
           -- partition by payment_id
            --order by created_at desc
        --) as rn
    from source
    where payment_id is not null
      and trip_id is not null
      and amount is not null

),


final as (

    select
        cast(payment_id as int64) as payment_id,
        cast(trip_id as int64) as trip_id,
        cast(amount as float64) as amount,
        cast(fee as float64) as fee,
        cast(currency as string) as currency,
        cast(payment_provider as string) as payment_provider,
        cast(payment_status as string) as payment_status,
        cast(created_at as timestamp) as created_at
    from cleaned
    where rn = 1

)

select * from final
