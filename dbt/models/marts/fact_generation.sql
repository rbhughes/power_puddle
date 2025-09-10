{{ config(materialized='table') }}


with generation_facts as (
    select
        -- Dimension keys
        plant_id_eia,
        report_date,
        fuel_type_code_pudl || '|' || energy_source_code || '|' || prime_mover_code as fuel_dim_key,
        
        -- Measures - Generation
        sum(net_generation_mwh) as net_generation_mwh,
        
        -- Measures - Fuel Consumption
        sum(fuel_consumed_mmbtu) as fuel_consumed_mmbtu,
        sum(fuel_consumed_for_electricity_mmbtu) as fuel_consumed_for_electricity_mmbtu,
        sum(fuel_consumed_units) as fuel_consumed_units,
        sum(fuel_consumed_for_electricity_units) as fuel_consumed_for_electricity_units,
        
        -- Calculated measures
        case 
            when sum(fuel_consumed_units) > 0 then
                sum(fuel_mmbtu_per_unit * fuel_consumed_units) / sum(fuel_consumed_units)
            else null
        end as avg_fuel_mmbtu_per_unit,
        
        case 
            when sum(net_generation_mwh) > 0 and sum(fuel_consumed_for_electricity_mmbtu) > 0 then
                (sum(fuel_consumed_for_electricity_mmbtu) * 1000000) / (sum(net_generation_mwh) * 1000)
            else null
        end as heat_rate_btu_per_kwh,
        
        -- Metadata
        count(distinct generator_composite_id) as generator_count,
        count(distinct nuclear_unit_id) as nuclear_unit_count,
        sum(case when data_maturity = 'final' then 1 else 0 end) as final_records,
        sum(case when data_maturity != 'final' then 1 else 0 end) as provisional_records,
        count(*) as total_records
        
    from {{ ref('int_generation_combined') }}
    group by 1, 2, 3
),

-- Add percentage calculations at plant level
generation_with_plant_totals as (
    select
        g.*,
        -- Plant-level percentages for the month
        g.net_generation_mwh / sum(g.net_generation_mwh) over (partition by g.plant_id_eia, g.report_date) as pct_of_plant_generation,
        
        -- State-level percentages for the month (requires plant dimension)
        g.net_generation_mwh / sum(g.net_generation_mwh) over (partition by p.state, g.report_date) as pct_of_state_generation
        
    from generation_facts g
    left join {{ ref('dim_plant') }} p on g.plant_id_eia = p.plant_id_eia
)

select * from generation_with_plant_totals
