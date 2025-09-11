-- models/marts/dashboard_county_energy_datacenter_density.sql  
{{ config(materialized='table') }}

with county_datacenters as (
    select 
        -- Extract county from address (simplified - you may need to clean this)
        trim(split_part(address, ',', -2)) as county_name,
        count(*) as datacenter_count,
        count(*) * 7.5 as estimated_total_demand_mw  -- Conservative estimate
    from {{ ref('dim_data_center') }}
    group by 1
),

county_generation as (
    select 
        p.county,
        f.fuel_category,
        sum(g.net_generation_mwh) as total_generation_mwh,
        avg(g.heat_rate_btu_per_kwh) as avg_heat_rate,
        count(distinct g.plant_id_eia) as plant_count
    from {{ ref('fact_generation') }} g
    join {{ ref('dim_plant') }} p on g.plant_id_eia = p.plant_id_eia  
    join {{ ref('dim_fuel') }} f on g.fuel_dim_key = f.fuel_dim_key
    join {{ ref('dim_time') }} t on g.report_date = t.report_date
    where p.state = 'IL' and t.report_year = 2023
    group by 1,2
)

select 
    coalesce(cg.county, cd.county_name) as county,
    coalesce(cd.datacenter_count, 0) as datacenter_count,
    coalesce(cd.estimated_total_demand_mw, 0) as estimated_datacenter_demand_mw,
    cg.fuel_category,
    coalesce(cg.total_generation_mwh, 0) as county_generation_mwh,
    coalesce(cg.plant_count, 0) as power_plants_in_county,
    
    -- Energy self-sufficiency ratio
    case 
        when cd.estimated_total_demand_mw > 0 and cg.total_generation_mwh > 0 then
            (cg.total_generation_mwh / (cd.estimated_total_demand_mw * 24 * 365 * 0.8)) * 100
        else null
    end as generation_to_datacenter_demand_ratio

from county_generation cg
full outer join county_datacenters cd on cg.county = cd.county_name
