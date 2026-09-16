-- Hourly fact table: real demand vs REE's own forecast plus prices and
-- calendar features. This is the training table for the demand-forecast model
-- (phase 2.5) and the benchmark base: our model vs forecast_ree.
with demand as (

    select
        date_trunc('hour', measured_at_local)                    as hour_local,
        avg(case when series = 'real' then demand_mw end)        as demand_real_mw,
        avg(case when series = 'forecast_ree' then demand_mw end) as demand_forecast_ree_mw
    from {{ ref('stg_ree__demand') }}
    group by 1

),

prices as (

    select
        date_trunc('hour', priced_at_local)                       as hour_local,
        avg(case when market = 'pvpc' then price_eur_mwh end)     as price_pvpc_eur_mwh,
        avg(case when market = 'spot' then price_eur_mwh end)     as price_spot_eur_mwh
    from {{ ref('stg_ree__prices') }}
    group by 1

)

select
    demand.hour_local,
    demand.demand_real_mw,
    demand.demand_forecast_ree_mw,
    prices.price_pvpc_eur_mwh,
    prices.price_spot_eur_mwh,
    -- calendar features for the forecasting model
    extract(hour from demand.hour_local)      as hour_of_day,
    extract(isodow from demand.hour_local)    as day_of_week,
    extract(month from demand.hour_local)     as month_of_year,
    extract(isodow from demand.hour_local) >= 6 as is_weekend
from demand
left join prices using (hour_local)
