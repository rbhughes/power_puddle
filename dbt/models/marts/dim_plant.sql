{{ config(materialized='table') }}

with plant_base as (
    select
        plant_id_eia,
        plant_name_eia,
        city,
        county,
        state,
        street_address,
        zip_code,
        timezone,
        latitude,
        longitude
    from {{ source('power_puddle', 'core_eia_entity_plants') }}
),

plant_generation_summary as (
    select
        plant_id_eia,
        min(report_date) as first_generation_date,
        max(report_date) as last_generation_date,
        count(distinct fuel_type_code_pudl) as fuel_types_count,
        array_agg(distinct fuel_type_code_pudl) as fuel_types_list
    from {{ ref('int_generation_combined') }}
    group by plant_id_eia
)

select
    p.*,
    -- Generation activity metadata
    coalesce(g.first_generation_date, '1900-01-01'::date) as first_generation_date,
    coalesce(g.last_generation_date, '1900-01-01'::date) as last_generation_date,
    coalesce(g.fuel_types_count, 0) as fuel_types_count,
    coalesce(g.fuel_types_list, []) as fuel_types_list,
    
    -- Plant classification
    case
        when g.fuel_types_count > 3 then 'Multi-Fuel'
        when g.fuel_types_count = 1 then 'Single-Fuel'
        when g.fuel_types_count between 2 and 3 then 'Dual/Triple-Fuel'
        else 'No Generation Data'
    end as plant_fuel_classification,
    
    -- Regional grouping
    case 
        when state in ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') then 'New England'
        when state in ('NY', 'NJ', 'PA') then 'Mid-Atlantic'
        when state in ('OH', 'MI', 'IN', 'WI', 'IL', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') then 'Midwest'
        when state in ('DE', 'MD', 'DC', 'VA', 'WV', 'KY', 'TN', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'AR', 'LA') then 'South'
        when state in ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV', 'WA', 'OR', 'CA', 'AK', 'HI') then 'West'
        else 'Other'
    end as census_region

from plant_base p
left join plant_generation_summary g on p.plant_id_eia = g.plant_id_eia
