{{
    config(
        materialized = "view"
    )
}}

select 
    arrival_time as expected_arrival_time,
    departure_time as expected_departure_time,
    CASE WHEN drop_off_type = '0' THEN 'Regularly scheduled drop off' else 'Unknown' end as stop_drop_off_type,
    CASE WHEN pickup_type = '0' THEN 'Regularly scheduled drop off' else 'Unknown' end as stop_pickup_type,
    shape_dist_traveled as distance_traveled,
    stop_id,
    stop_sequence,
    timepoint,
    trip_id
from {{ source('raw_data', 'staging_stop_times') }} 