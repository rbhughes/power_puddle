-- models/marts/dashboard_illinois_clean_energy_datacenter.sql
{{ config(materialized='table') }}

with illinois_fuel_mix as (
    select 
        t.report_year,
        t.report_month,
        f.fuel_category,
        sum(g.net_generation_mwh) as generation_mwh,
        sum(g.net_generation_mwh) / sum(sum(g.net_generation_mwh)) over (partition by t.report_year, t.report_month) * 100 as fuel_mix_percentage
    from {{ ref('fact_generation') }} g
    join {{ ref('dim_plant') }} p on g.plant_id_eia = p.plant_id_eia
    join {{ ref('dim_fuel') }} f on g.fuel_dim_key = f.fuel_dim_key
    join {{ ref('dim_time') }} t on g.report_date = t.report_date
    where p.state = 'IL'
    group by 1,2,3
),

renewable_summary as (
    select 
        report_year,
        sum(case when fuel_category in ('Wind', 'Solar', 'Hydro') then generation_mwh else 0 end) as renewable_generation_mwh,
        sum(generation_mwh) as total_generation_mwh,
        sum(case when fuel_category in ('Wind', 'Solar', 'Hydro') then generation_mwh else 0 end) / sum(generation_mwh) * 100 as renewable_percentage
    from illinois_fuel_mix
    group by 1
)

select 
    fm.*,
    rs.renewable_percentage,
    -- Static data center count (could be made dynamic with time-series data)
    154 as current_data_centers,
    -- Policy context flags
    case when fm.report_year >= 2017 then 'Post-FEJA' else 'Pre-FEJA' end as feja_policy_period,
    case when fm.report_year >= 2021 then 'Post-CEJA' else 'Pre-CEJA' end as ceja_policy_period
from illinois_fuel_mix fm
left join renewable_summary rs on fm.report_year = rs.report_year
