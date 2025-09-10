{{ config(materialized='view') }}

with source as (
    select * from {{ source('power_puddle', 'core_eia923_monthly_generation_fuel') }}
),

plants as (
    select * from {{ source('power_puddle', 'core_eia_entity_plants') }}
),

cleaned as (
    select
        -- Identifiers
        s.plant_id_eia,
        s.report_date,
        
        -- Plant information from plants table
        p.plant_name_eia,
        p.state,
        p.county,
        p.city,
        p.latitude as plant_latitude,
        p.longitude as plant_longitude,
        p.timezone,
        
        -- Fuel information
        s.energy_source_code,
        s.fuel_type_code_pudl,
        s.fuel_type_code_agg,
        s.prime_mover_code,
        
        -- Generation metrics
        s.net_generation_mwh,
        s.fuel_consumed_mmbtu,
        s.fuel_consumed_for_electricity_mmbtu,
        s.fuel_consumed_units,
        s.fuel_consumed_for_electricity_units,
        s.fuel_mmbtu_per_unit,
        
        -- Data quality
        s.data_maturity,
        
        -- Derived time fields
        extract(year from s.report_date) as report_year,
        extract(month from s.report_date) as report_month,
        date_trunc('month', s.report_date) as report_month_start,
        
        -- Fuel category mapping using fuel_type_code_pudl
        case 
            when s.fuel_type_code_pudl = 'coal' then 'Coal'
            when s.fuel_type_code_pudl = 'gas' then 'Natural Gas'
            when s.fuel_type_code_pudl = 'nuclear' then 'Nuclear'
            when s.fuel_type_code_pudl = 'oil' then 'Oil'
            when s.fuel_type_code_pudl = 'hydro' then 'Hydro'
            when s.fuel_type_code_pudl = 'wind' then 'Wind'
            when s.fuel_type_code_pudl = 'solar' then 'Solar'
            --when s.fuel_type_code_pudl = 'geothermal' then 'Geothermal'
            when s.fuel_type_code_pudl = 'waste' then 'Biomass'
            when s.fuel_type_code_pudl = 'other' then 'Other'
            else 'Unknown'
        end as fuel_category,
        
        -- Generator identifier (we don't have this, so create composite)
        s.plant_id_eia || '_' || s.energy_source_code || '_' || s.prime_mover_code as generator_composite_id,
        
        -- Source indicator
        'regular' as generation_source
        
    from source s
    left join plants p on s.plant_id_eia = p.plant_id_eia
    where s.net_generation_mwh is not null
      and s.net_generation_mwh >= 0
)

select * from cleaned
