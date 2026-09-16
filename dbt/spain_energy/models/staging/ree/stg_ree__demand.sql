-- Electricity demand at 5-minute resolution, one row per (instant, series).
-- Timestamps are kept in Madrid wall-clock time (the API's local offset is
-- stripped): analyses care about "hour 20 local", not UTC.
with source as (

    select * from {{ source('ree', 'raw_ree_indicators') }}
    where indicator = 'demand_realtime'

),

renamed as (

    select
        strptime(substr(datetime_local, 1, 19), '%Y-%m-%dT%H:%M:%S') as measured_at_local,
        case series_title
            when 'Real' then 'real'
            when 'Prevista' then 'forecast_ree'
            when 'Programada' then 'scheduled'
            when 'Programada total' then 'scheduled_total'
        end                                                          as series,
        value                                                        as demand_mw
    from source

)

select * from renamed
