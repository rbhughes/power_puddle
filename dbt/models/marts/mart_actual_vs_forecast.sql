{{ config(
    materialized='table'
) }}

with il_actuals as (
    select
        extract(year from report_date) as year,
        extract(month from report_date) as month,
        report_date::date as report_date,
        sum(total_net_generation_mwh) / 1000 as actual_generation_gwh
    from {{ ref('mart_monthly_generation_summary') }}
    where state = 'IL'
      and county in (
        'Boone',
        'Bureau',
        'Carroll',
        'Cook',
        'DeKalb',
        'DuPage',
        'Ford',
        'Grundy',
        'Henry',
        'Iroquois',
        'Jo Daviess',
        'Kane',
        'Kankakee',
        'Kendall',
        'Lake',
        'LaSalle',
        'Lee',
        'Livingston',
        'Marshall',
        'McHenry',
        'Ogle',
        'Putnam',
        'Rock Island',
        'Stephenson',
        'Will',
        'Winnebago'
      )
    group by report_date
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
    coalesce(a.report_date, f.report_date) as report_date,
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
where f.forecast_year < 2031
group by f.forecast_year, f.month, coalesce(a.report_date, f.report_date), a.actual_generation_gwh
order by f.forecast_year, f.month
