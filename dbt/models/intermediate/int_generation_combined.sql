{{ config(materialized='table') }}

with regular_generation as (
    select
        plant_id_eia,
        generator_composite_id,
        report_date,
        report_year,
        report_month,
        report_month_start,
        plant_name_eia,
        state,
        county,
        city,
        plant_latitude,
        plant_longitude,
        timezone,
        energy_source_code,
        fuel_type_code_pudl,
        fuel_type_code_agg,
        prime_mover_code,
        fuel_category,
        net_generation_mwh,
        fuel_consumed_mmbtu,
        fuel_consumed_for_electricity_mmbtu,
        fuel_consumed_units,
        fuel_consumed_for_electricity_units,
        fuel_mmbtu_per_unit,
        data_maturity,
        generation_source,
        null as nuclear_unit_id
    from {{ ref('stg_monthly_gen') }}
),

nuclear_generation as (
    select
        plant_id_eia,
        generator_composite_id,
        report_date,
        report_year,
        report_month,
        report_month_start,
        plant_name_eia,
        state,
        county,
        city,
        plant_latitude,
        plant_longitude,
        timezone,
        energy_source_code,
        fuel_type_code_pudl,
        fuel_type_code_agg,
        prime_mover_code,
        fuel_category,
        net_generation_mwh,
        fuel_consumed_mmbtu,
        fuel_consumed_for_electricity_mmbtu,
        fuel_consumed_units,
        fuel_consumed_for_electricity_units,
        fuel_mmbtu_per_unit,
        data_maturity,
        generation_source,
        nuclear_unit_id
    from {{ ref('stg_monthly_gen_nuc') }}
),

combined as (
    select * from regular_generation
    union all
    select * from nuclear_generation
)

select * from combined
