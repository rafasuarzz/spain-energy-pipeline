-- Hourly electricity demand (real / forecast / scheduled series).
with source as (

    select * from {{ source('ree', 'raw_ree_indicators') }}
    where indicator = 'demand_realtime'

),

renamed as (

    select
        series_title                                as series,
        cast(datetime_local as timestamp with time zone) as measured_at,
        value                                       as demand_mw,
        cast(extraction_day as date)                as extraction_day
    from source

)

select * from renamed
