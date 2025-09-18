with il_actuals as (
    select
        report_date::date as report_date,
        sum(total_net_generation_mwh) / 1000 as actual_generation_gwh
    from {{ ref('mart_monthly_generation_summary') }}
    where state = 'IL'
      and county in (
        'Boone','Bureau','Carroll','Cook','DeKalb','DuPage','Ford','Grundy','Henry',
        'Iroquois','Jo Daviess','Kane','Kankakee','Kendall','Lake','LaSalle','Lee',
        'Livingston','Marshall','McHenry','Ogle','Putnam','Rock Island','Stephenson',
        'Will','Winnebago'
      )
    group by report_date
),

il_forecasts as (
    select
        make_date(year, month, 1) as report_date,
        energy_gwh as forecast_generation_gwh
    from forecasts
    where zone_name = 'COMED'
    and energy_gwh is not null
)

select
    a.report_date,
    a.actual_generation_gwh,
    f.forecast_generation_gwh
from il_actuals a
join il_forecasts f
  on a.report_date = f.report_date
order by a.report_date