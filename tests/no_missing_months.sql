-- Test for gaps in months in the mart_monthly_generation_summary table

WITH date_bounds AS (
    SELECT
        min(report_date)::date AS min_date,
        max(report_date)::date AS max_date
    FROM {{ ref('mart_monthly_generation_summary') }}
),

expected_dates AS (
    SELECT
        (min_date + INTERVAL (seq) MONTH)::date AS report_date
    FROM date_bounds,
    UNNEST(range(0, date_diff('month', min_date, max_date) + 1)) AS t(seq)
),

actual_dates AS (
    SELECT DISTINCT report_date FROM {{ ref('mart_monthly_generation_summary') }}
),

missing_dates AS (
    SELECT report_date
    FROM expected_dates
    WHERE report_date NOT IN (SELECT report_date FROM actual_dates)
)

SELECT * FROM missing_dates
