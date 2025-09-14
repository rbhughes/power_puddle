{{ config(materialized='table') }}

WITH recent_generation AS (
    SELECT
        g.plant_id_eia,
        SUM(g.net_generation_mwh) AS total_generation_last_year,
        f.fuel_category AS primary_fuel_category,
        ROW_NUMBER() OVER (PARTITION BY g.plant_id_eia ORDER BY SUM(g.net_generation_mwh) DESC) AS rn
    FROM {{ ref('fact_generation') }} g
    JOIN {{ ref('dim_fuel') }} f ON g.fuel_dim_key = f.fuel_dim_key
    JOIN {{ ref('dim_time') }} t ON g.report_date = t.report_date
    WHERE t.report_year = 2023 -- Most recent year
    GROUP BY 1, 3
)

SELECT
    dc.data_center_name,
    dc.address AS datacenter_address,
    dc.latitude AS datacenter_latitude,
    dc.longitude AS datacenter_longitude,
    p.plant_name_eia AS nearest_plant_name,
    p.county AS plant_county,
    p.latitude AS plant_latitude,
    p.longitude AS plant_longitude,
    dc.nearest_plant_distance_miles,
    -- Plant characteristics
    p.fuel_types_list AS available_fuel_types,
    p.plant_fuel_classification,
    -- Recent generation from nearest plant
    COALESCE(rg.total_generation_last_year, 0) AS nearest_plant_generation_mwh_last_year,
    COALESCE(rg.primary_fuel_category, 'No Recent Data') AS nearest_plant_primary_fuel,
    -- Carbon intensity of nearest power source
    CASE
        WHEN COALESCE(rg.primary_fuel_category, '') IN ('Coal') THEN 'High Carbon'
        WHEN COALESCE(rg.primary_fuel_category, '') IN ('Natural Gas', 'Petroleum') THEN 'Medium Carbon'
        WHEN COALESCE(rg.primary_fuel_category, '') IN ('Nuclear', 'Wind', 'Solar', 'Hydro') THEN 'Low Carbon'
        ELSE 'Unknown'
    END AS nearest_plant_carbon_profile
FROM {{ ref('dim_data_center') }} dc
LEFT JOIN {{ ref('dim_plant') }} p ON dc.nearest_plant_id_eia = p.plant_id_eia
LEFT JOIN recent_generation rg ON p.plant_id_eia = rg.plant_id_eia AND rg.rn = 1
