{% snapshot drivers_snapshot %}

{{
    config(
        target_schema='beejan_dataset',
        unique_key='driver_id',

        strategy='check',

        check_cols=[
            'driver_status',
            'vehicle_id',
            'rating'
        ]
    )
}}

select
    driver_id,
    --city_id,
    vehicle_id,
    driver_status,
    rating,
    --created_at,
    --updated_at,
    --onboarding_date

from {{ ref('stg_drivers_raw') }}

{% endsnapshot %}