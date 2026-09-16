-- Electricity market prices (EUR/MWh): PVPC (hourly, retail regulated tariff)
-- and the wholesale spot market (quarter-hourly since the 2025 EU market change).
with source as (

    select * from {{ source('ree', 'raw_ree_indicators') }}
    where indicator = 'market_prices'

),

renamed as (

    select
        strptime(substr(datetime_local, 1, 19), '%Y-%m-%dT%H:%M:%S') as priced_at_local,
        case series_title
            when 'PVPC' then 'pvpc'
            when 'Precio mercado spot' then 'spot'
        end                                                          as market,
        value                                                        as price_eur_mwh
    from source

)

select * from renamed
