{{
    config(
        materialized = "view"
    )
}}

select 
    vehicle_id as sk_vehicle,
    trip_id as sk_trip,
    {{ dbt_utils.generate_surrogate_key(['stop_id', 'current_time']) }} as sk_stop,
    {{ dbt_utils.generate_surrogate_key(['line_id','route_id','current_time']) }} AS sk_line,  
    {{ dbt_utils.generate_surrogate_key(['route_id','current_time']) }} as sk_route,
    current_status,
    DATE(timestamp) as sk_date,
    timestamp as sk_hist_datetime
from {{ source('raw_data', 'staging_historical_stop_times') }} 