{{ config(materialized='table') }}

select 
    data_center_name,
    latitude as datacenter_latitude,
    longitude as datacenter_longitude
from {{ ref('dim_data_center') }}
where latitude is not null 
  and longitude is not null