{{ config(materialized='table') }}

with fuel_combinations as (
    select distinct
        fuel_type_code_pudl,
        energy_source_code,
        prime_mover_code,
        fuel_category
    from {{ ref('int_generation_combined') }}
),

fuel_attributes as (
    select
        -- Natural key
        fuel_type_code_pudl || '|' || energy_source_code || '|' || prime_mover_code as fuel_dim_key,
        
        -- Fuel attributes
        fuel_type_code_pudl,
        energy_source_code,
        prime_mover_code,
        fuel_category,
        
        -- Fuel type classification
        case 
            when fuel_type_code_pudl in ('coal') then 'Fossil'
            when fuel_type_code_pudl in ('gas', 'oil') then 'Fossil'
            when fuel_type_code_pudl in ('wind', 'solar', 'hydro') then 'Renewable'
            when fuel_type_code_pudl in ('waste') then 'Renewable'
            when fuel_type_code_pudl = 'nuclear' then 'Nuclear'
            else 'Other'
        end as fuel_source_type,
        
        -- Carbon intensity classification
        case 
            when fuel_type_code_pudl = 'coal' then 'High Carbon'
            when fuel_type_code_pudl in ('gas', 'oil') then 'Medium Carbon'
            when fuel_type_code_pudl in ('wind', 'solar', 'hydro', 'nuclear') then 'Low/Zero Carbon'
            when fuel_type_code_pudl = 'waste' then 'Carbon Neutral'
            else 'Unknown Carbon'
        end as carbon_intensity,
        
        -- Prime mover type description
        case 
            when prime_mover_code = 'ST' then 'Steam Turbine'
            when prime_mover_code = 'GT' then 'Gas Turbine'
            when prime_mover_code = 'IC' then 'Internal Combustion'
            when prime_mover_code = 'CC' then 'Combined Cycle'
            when prime_mover_code = 'WT' then 'Wind Turbine'
            when prime_mover_code = 'PV' then 'Photovoltaic'
            when prime_mover_code = 'HY' then 'Hydroelectric'
            else 'Other/Unknown'
        end as prime_mover_description,
        
        -- Dispatchability
        case 
            when fuel_type_code_pudl in ('wind', 'solar') then 'Variable'
            when fuel_type_code_pudl = 'hydro' then 'Semi-Dispatchable'
            when fuel_type_code_pudl in ('coal', 'gas', 'oil', 'nuclear') then 'Dispatchable'
            else 'Unknown'
        end as dispatchability
        
    from fuel_combinations
)

select * from fuel_attributes
order by fuel_category, fuel_type_code_pudl
