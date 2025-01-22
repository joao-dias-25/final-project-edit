{{
    config(
        materialized = "view"
    )
}}

select 
calendar_desc,
CASE 
    WHEN direction_id = '0' THEN 'outbound travel'
    WHEN direction_id = '1' THEN 'inbound travel' 
    ELSE 'Unknown' end as direction_id
,
pattern_id,
route_id as sk_route,
service_id,
shape_id,
trip_headsign,
trip_id
from {{ source('raw_data', 'staging_trips') }} a