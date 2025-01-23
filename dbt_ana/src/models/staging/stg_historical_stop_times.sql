{{
    config(
        materialized = "view"
    )
}}

select 
    vehicle_id as sk_vehicle,
    trip_id as sk_trip,
    stop_id as sk_stop,
    line_id as sk_line,
    route_id as sk_route,
    current_status,
    DATE(timestamp) as sk_date,
    timestamp as sk_hist_datetime
from {{ source('raw_data', 'staging_historical_stop_times') }} 