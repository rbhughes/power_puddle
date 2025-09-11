{{ config(materialized='table') }}

select 
    p.plant_name_eia,
    p.state,
    p.census_region,
    f.carbon_intensity,
    f.fuel_category,
    sum(g.net_generation_mwh) as total_generation_mwh,
    avg(g.heat_rate_btu_per_kwh) as avg_heat_rate,
    sum(g.fuel_consumed_mmbtu) as total_fuel_mmbtu,
    
    -- Pre-calculate dashboard metrics
    rank() over (order by sum(g.net_generation_mwh) desc) as generation_rank,
    sum(g.net_generation_mwh) / sum(sum(g.net_generation_mwh)) over () * 100 as pct_of_total_generation
    
from {{ ref('fact_generation') }} g
join {{ ref('dim_plant') }} p on g.plant_id_eia = p.plant_id_eia
join {{ ref('dim_fuel') }} f on g.fuel_dim_key = f.fuel_dim_key
join {{ ref('dim_time') }} t on g.report_date = t.report_date
where f.carbon_intensity = 'High Carbon'
  and t.report_year >= 2020
group by 1,2,3,4,5
order by total_generation_mwh desc
