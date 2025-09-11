{{ config(materialized='table') }}

with recent_generation as (
    select 
        g.plant_id_eia,
        sum(g.net_generation_mwh) as total_generation_last_year,
        f.fuel_category as primary_fuel_category,
        row_number() over (partition by g.plant_id_eia order by sum(g.net_generation_mwh) desc) as rn
    from {{ ref('fact_generation') }} g
    join {{ ref('dim_fuel') }} f on g.fuel_dim_key = f.fuel_dim_key
    join {{ ref('dim_time') }} t on g.report_date = t.report_date
    where t.report_year = 2023  -- Most recent year
    group by 1, 3
)

select 
    dc.data_center_name,
    dc.address as datacenter_address,
    p.plant_name_eia as nearest_plant_name,
    p.county as plant_county,
    dc.nearest_plant_distance_miles,
    
    -- Plant characteristics
    p.fuel_types_list as available_fuel_types,
    p.plant_fuel_classification,
    
    -- Recent generation from nearest plant
    coalesce(rg.total_generation_last_year, 0) as nearest_plant_generation_mwh_last_year,
    coalesce(rg.primary_fuel_category, 'No Recent Data') as nearest_plant_primary_fuel,
    
    -- Carbon intensity of nearest power source
    case 
        when coalesce(rg.primary_fuel_category, '') in ('Coal') then 'High Carbon'
        when coalesce(rg.primary_fuel_category, '') in ('Natural Gas', 'Petroleum') then 'Medium Carbon'  
        when coalesce(rg.primary_fuel_category, '') in ('Nuclear', 'Wind', 'Solar', 'Hydro') then 'Low Carbon'
        else 'Unknown'
    end as nearest_plant_carbon_profile

from {{ ref('dim_data_center') }} dc
left join {{ ref('dim_plant') }} p on dc.nearest_plant_id_eia = p.plant_id_eia
left join recent_generation rg on p.plant_id_eia = rg.plant_id_eia and rg.rn = 1
