{{ config(materialized='table') }}

with date_spine as (
    -- Generate date series manually for DuckDB
    select 
        date_add('2020-01-01'::date, interval (seq) month) as date_month
    from generate_series(0, 120) as t(seq)  -- 10 years of months
),

time_attributes as (
    select
        date_month as report_date,
        extract(year from date_month) as report_year,
        extract(month from date_month) as report_month,
        extract(quarter from date_month) as report_quarter,
        
        -- Month names and abbreviations using DuckDB's strftime()
        strftime(date_month, '%B') as month_name,        -- Full month name
        substr(strftime(date_month, '%B'), 1, 3) as month_abbr,  -- First 3 chars
        
        -- Quarter labels
        'Q' || extract(quarter from date_month) as quarter_label,
        extract(year from date_month) || '-Q' || extract(quarter from date_month) as year_quarter,
        
        -- Fiscal year (Oct-Sep)
        case 
            when extract(month from date_month) >= 10 then extract(year from date_month) + 1
            else extract(year from date_month)
        end as fiscal_year,
        
        -- Season classification
        case 
            when extract(month from date_month) in (12, 1, 2) then 'Winter'
            when extract(month from date_month) in (3, 4, 5) then 'Spring'
            when extract(month from date_month) in (6, 7, 8) then 'Summer'
            when extract(month from date_month) in (9, 10, 11) then 'Fall'
        end as season,
        
        -- Peak demand periods
        case 
            when extract(month from date_month) in (6, 7, 8) then 'Summer Peak'
            when extract(month from date_month) in (12, 1, 2) then 'Winter Peak'
            else 'Off-Peak'
        end as demand_period
        
    from date_spine
)

select * from time_attributes
order by report_date
