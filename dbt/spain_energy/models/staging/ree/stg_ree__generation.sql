-- Daily generation (MWh) by technology, classified renewable / non-renewable
-- via the generation_technologies seed. The API's own total row is excluded:
-- totals belong in marts, computed from the parts.
with source as (

    select * from {{ source('ree', 'raw_ree_indicators') }}
    where indicator = 'generation_mix'
      and series_title != 'Generación total'

),

technologies as (

    select * from {{ ref('generation_technologies') }}

),

joined as (

    select
        cast(source.extraction_day as date) as generation_date,
        source.series_title                 as technology,
        technologies.is_renewable,
        source.value                        as generation_mwh,
        source.percentage                   as share_of_total
    from source
    left join technologies
        on source.series_title = technologies.technology

)

select * from joined
