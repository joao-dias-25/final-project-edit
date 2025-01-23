{{
    config(
        materialized = "view",
    )
}}

with stops as (
        select 
            stop_id,
            stop_name,
            stop_short_name,
            stop_lat          AS stop_latitude,
            stop_lon          AS stop_longitude,
            region_id,
            region_name,
            district_id,
            district_name,
            municipality_id,
            municipality_name,
            locality,
            operational_status,
            CASE 
            WHEN near_school = '0' THEN 'No'
            WHEN near_school = '1' THEN 'Yes' 
            ELSE 'N/A'
            END                 AS near_school,
            CASE 
            WHEN airport  = '0' THEN 'No'
            WHEN airport  = '1' THEN 'Yes' 
            ELSE 'N/A'
            END                 AS near_airport,
            CASE 
            WHEN subway  = '0' THEN 'No'
            WHEN subway  = '1' THEN 'Yes' 
            ELSE 'N/A'
            END                 AS near_subway,
            CASE 
            WHEN train  = '0' THEN 'No'
            WHEN train  = '1' THEN 'Yes' 
            ELSE 'N/A'
            END                  AS near_train
        from {{ source('raw_data', 'staging_stops') }}
    )

select *
from stops



