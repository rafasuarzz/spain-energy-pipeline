-- Total daily electricity demand (MWh), one row per day.
with source as (

    select * from {{ source('ree', 'raw_ree_indicators') }}
    where indicator = 'demand_daily'

),

renamed as (

    select
        cast(extraction_day as date) as demand_date,
        value                        as demand_mwh
    from source

)

select * from renamed
