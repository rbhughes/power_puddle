{{ config(materialized='table') }}

with data_center_base as (
    select
        row_number() over (order by name, address) as data_center_id,
        name as data_center_name,
        address,
        latitude,
        longitude
    from {{ source('power_puddle', 'data_centers') }}
)

-- Meh. Proximity does not mean that a DC gets power from the closest plant

-- Find nearest power plants to each data center
-- data_centers_with_nearest_plants as (
--     select
--         dc.*,
        
--         -- Find distance to nearest power plant
--         (select 
--             plant_id_eia
--          from {{ ref('dim_plant') }} p
--          where p.latitude is not null and p.longitude is not null
--          order by sqrt(power((p.latitude - dc.latitude), 2) + power((p.longitude - dc.longitude), 2))
--          limit 1
--         ) as nearest_plant_id_eia,
        
--         -- Calculate distance to nearest plant (approximate)
--         (select 
--             round(sqrt(power((p.latitude - dc.latitude), 2) + power((p.longitude - dc.longitude), 2)) * 69, 2)
--          from {{ ref('dim_plant') }} p
--          where p.latitude is not null and p.longitude is not null
--          order by sqrt(power((p.latitude - dc.latitude), 2) + power((p.longitude - dc.longitude), 2))
--          limit 1
--         ) as nearest_plant_distance_miles
        
--     from data_center_base dc
-- )

-- select * from data_centers_with_nearest_plants
select * from data_center_base
