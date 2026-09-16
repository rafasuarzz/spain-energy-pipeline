-- Daily fact table joining demand, generation mix and prices.
-- Base for the merit-order analysis: does a higher renewable share
-- push the spot price down?
with demand as (

    select demand_date, demand_mwh
    from {{ ref('stg_ree__demand_daily') }}

),

generation as (

    select
        generation_date,
        sum(generation_mwh)                                            as generation_total_mwh,
        sum(case when is_renewable then generation_mwh else 0 end)     as generation_renewable_mwh,
        sum(case when not is_renewable then generation_mwh else 0 end) as generation_non_renewable_mwh
    from {{ ref('stg_ree__generation') }}
    group by 1

),

prices as (

    select
        cast(date_trunc('day', priced_at_local) as date)          as price_date,
        avg(case when market = 'pvpc' then price_eur_mwh end)     as price_pvpc_avg_eur_mwh,
        avg(case when market = 'spot' then price_eur_mwh end)     as price_spot_avg_eur_mwh,
        min(case when market = 'spot' then price_eur_mwh end)     as price_spot_min_eur_mwh,
        max(case when market = 'spot' then price_eur_mwh end)     as price_spot_max_eur_mwh
    from {{ ref('stg_ree__prices') }}
    group by 1

)

select
    demand.demand_date                                                     as energy_date,
    demand.demand_mwh,
    generation.generation_total_mwh,
    generation.generation_renewable_mwh,
    generation.generation_non_renewable_mwh,
    generation.generation_renewable_mwh / nullif(generation.generation_total_mwh, 0)
                                                                           as renewable_share,
    prices.price_pvpc_avg_eur_mwh,
    prices.price_spot_avg_eur_mwh,
    prices.price_spot_min_eur_mwh,
    prices.price_spot_max_eur_mwh,
    extract(isodow from demand.demand_date)                                as day_of_week,
    extract(isodow from demand.demand_date) >= 6                           as is_weekend
from demand
left join generation on demand.demand_date = generation.generation_date
left join prices on demand.demand_date = prices.price_date
