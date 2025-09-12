{{ config(
    materialized='table'
) }}

# CHP Context Consideration
# The model correctly uses fuel_consumed_for_electricity_mmbtu 
# rather than total fuel consumed for heat rate calculations, 
# which properly handles CHP plants by only counting fuel used for 
# electricity generation.

with monthly_summary as (
    select
        -- Time dimensions
        report_date,                    -- 1
        report_year,                    -- 2
        report_month,                   -- 3
        report_month_start,             -- 4
        
        -- Geographic dimensions
        state,                          -- 5
        county,                         -- 6
        city,                           -- 7
        
        -- Plant dimension
        plant_id_eia,                   -- 8
        plant_name_eia,                 -- 9
        
        -- Fuel dimensions
        fuel_category,                  -- 10
        fuel_type_code_agg,             -- 11
        energy_source_code,             -- 12
        prime_mover_code,               -- 13
        
        -- Aggregated metrics (these are NOT in GROUP BY)
        sum(net_generation_mwh) as total_net_generation_mwh,
        sum(fuel_consumed_mmbtu) as total_fuel_consumed_mmbtu,
        sum(fuel_consumed_for_electricity_mmbtu) as total_fuel_consumed_for_electricity_mmbtu,
        sum(fuel_consumed_units) as total_fuel_consumed_units,
        sum(fuel_consumed_for_electricity_units) as total_fuel_consumed_for_electricity_units,
        
        -- Weighted average fuel heat content
        case 
            when sum(fuel_consumed_units) > 0 then
                sum(fuel_mmbtu_per_unit * fuel_consumed_units) / sum(fuel_consumed_units)
            else null
        end as avg_fuel_mmbtu_per_unit,
        
        -- Counts and metadata
        count(distinct generator_composite_id) as generator_count,
        count(distinct nuclear_unit_id) as nuclear_unit_count,
        count(*) as record_count,
        
        -- Data quality indicators
        sum(case when data_maturity = 'final' then 1 else 0 end) as final_records,
        sum(case when data_maturity != 'final' then 1 else 0 end) as provisional_records,
        
        -- Plant location (taking first non-null values)
        min(plant_latitude) as plant_latitude,
        min(plant_longitude) as plant_longitude
        
    from {{ ref('int_generation_combined') }}
    -- Only group by the first 13 columns (non-aggregates)
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),

with_derived_metrics as (
    select
        *,
        -- Heat rate calculation (Btu/kWh)
        case 
            when total_net_generation_mwh > 0 and total_fuel_consumed_for_electricity_mmbtu > 0 then
                (total_fuel_consumed_for_electricity_mmbtu * 1000000) / (total_net_generation_mwh * 1000)
            else null
        end as heat_rate_btu_per_kwh,
        
        -- Calculate days in month for capacity factor estimation
        case 
            when report_month_start is not null then
                extract(day from (report_month_start + interval '1 month' - interval '1 day'))
            else null
        end as days_in_month
        
    from monthly_summary
)

select * from with_derived_metrics
order by report_date desc, state, fuel_category, plant_id_eia
