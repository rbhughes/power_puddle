{{ config(
    materialized='table'
) }}

with il_actuals as (
    select
        extract(year from m.report_date) as year,
        extract(month from m.report_date) as month,
        make_date(extract(year from m.report_date), extract(month from m.report_date), 1) as report_date,
        sum(m.total_net_generation_mwh) / 1000 as actual_generation_gwh
    from {{ ref('mart_monthly_generation_summary') }} m
    where m.state = 'IL'
      and exists (
        select 1
        from puddle.main.pjm_resources pr
        where rapidfuzz_ratio(
            upper(m.plant_name_eia),
            case extract(year from m.report_date)
                when 2018 then pr."2018"
                when 2019 then pr."2019"
                when 2020 then pr."2020"
                when 2021 then pr."2021"
                when 2022 then pr."2022"
                when 2023 then pr."2023"
                when 2024 then pr."2024"
                when 2025 then pr."2025"
            end
        ) > 80
      )
    group by year, month, report_date
),

il_forecasts as (
    select
        publication_year,
        year as forecast_year,
        month,
        make_date(year, month, 1) as report_date,
        energy_gwh
    from puddle.main.pjm_forecasts
    where zone_name = 'COMED'
      and energy_gwh is not null
)

select
    a.report_date,
    f.forecast_year,
    f.month,
    a.actual_generation_gwh,
    max(case when f.publication_year = 2016 then f.energy_gwh end) as forecast_2016,
    max(case when f.publication_year = 2017 then f.energy_gwh end) as forecast_2017,
    max(case when f.publication_year = 2018 then f.energy_gwh end) as forecast_2018,
    max(case when f.publication_year = 2019 then f.energy_gwh end) as forecast_2019,
    max(case when f.publication_year = 2020 then f.energy_gwh end) as forecast_2020,
    max(case when f.publication_year = 2021 then f.energy_gwh end) as forecast_2021,
    max(case when f.publication_year = 2022 then f.energy_gwh end) as forecast_2022,
    max(case when f.publication_year = 2023 then f.energy_gwh end) as forecast_2023
from il_forecasts f
left join il_actuals a 
  on a.year = f.forecast_year and a.month = f.month
where f.forecast_year < 2025
group by f.forecast_year, f.month, a.report_date, a.actual_generation_gwh
order by f.forecast_year, f.month