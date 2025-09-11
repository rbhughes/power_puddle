-- models/marts/dashboard_illinois_datacenter_impact.sql
{{ config(materialized='table') }}

with illinois_generation as (
    select 
        t.report_date,
        t.report_year,
        sum(g.net_generation_mwh) as total_state_generation_mwh,
        sum(case when f.fuel_category = 'Coal' then g.net_generation_mwh else 0 end) as coal_generation_mwh,
        sum(case when f.carbon_intensity = 'High Carbon' then g.net_generation_mwh else 0 end) as high_carbon_generation_mwh
    from {{ ref('fact_generation') }} g
    join {{ ref('dim_plant') }} p on g.plant_id_eia = p.plant_id_eia
    join {{ ref('dim_fuel') }} f on g.fuel_dim_key = f.fuel_dim_key  
    join {{ ref('dim_time') }} t on g.report_date = t.report_date
    where p.state = 'IL'
    group by 1,2
),

datacenter_metrics as (
    select 
        count(*) as total_data_centers,
        count(distinct substring(address, -5)) as counties_with_datacenters,
        -- Estimate 5-10 MW per data center (conservative estimate)
        count(*) * 7.5 as estimated_datacenter_demand_mw
    from {{ ref('dim_data_center') }}
)

select 
    g.*,
    d.total_data_centers,
    d.counties_with_datacenters,
    d.estimated_datacenter_demand_mw,
    -- Calculate potential data center consumption (assume 80% capacity factor)
    (d.estimated_datacenter_demand_mw * 24 * 30 * 0.8) as estimated_monthly_datacenter_consumption_mwh,
    -- Percentage of state generation that could be used by data centers
    ((d.estimated_datacenter_demand_mw * 24 * 30 * 0.8) / g.total_state_generation_mwh * 100) as datacenter_pct_of_state_generation
from illinois_generation g
cross join datacenter_metrics d
