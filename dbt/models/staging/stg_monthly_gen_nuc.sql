{{ config(materialized='view') }}

with source as (
    select * from {{ source('power_puddle', 'core_eia923_monthly_generation_fuel_nuclear') }}
),

plants as (
    select * from {{ source('power_puddle', 'core_eia_entity_plants') }}
),

cleaned as (
    select
        -- Identifiers  
        s.plant_id_eia,
        s.report_date,
        s.nuclear_unit_id,
        
        -- Plant information from plants table
        p.plant_name_eia,
        p.state,
        p.county,
        p.city,
        p.latitude as plant_latitude,
        p.longitude as plant_longitude,
        p.timezone,
        
        -- Nuclear fuel information
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
        -- (for some reason, there are oil records in the nuc set)
        case 
            when s.fuel_type_code_pudl = 'nuclear' then 'Nuclear'
            when s.fuel_type_code_pudl = 'oil' then 'Oil'
            else 'Unknown'
        end as fuel_category,
        
        -- Generator identifier using nuclear unit
        s.plant_id_eia || '_' || s.nuclear_unit_id as generator_composite_id,
        
        -- Source indicator
        'nuclear' as generation_source
        
    from source s
    left join plants p on s.plant_id_eia = p.plant_id_eia
    where s.net_generation_mwh is not null
      and s.net_generation_mwh >= 0
)

select * from cleaned
