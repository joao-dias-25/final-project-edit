{{ 
    config(
        materialized = "table" 
    )
}}

WITH trip_service AS (
    Select * from (
        SELECT DISTINCT *, ROW_NUMBER() OVER (PARTITION BY stop_id, trip_id ORDER BY expected_arrival_time DESC) as row_
    FROM {{ ref('stg_stop_times') }} ) where row_=1  
),
trip_status AS (
    SELECT DISTINCT
        CONCAT(b.sk_stop, '_', b.sk_trip, '_', b.sk_hist_datetime) AS pk_trip_stop_id,
        b.sk_stop,
        b.sk_trip,
        b.sk_vehicle,
        b.sk_line,
        b.sk_route,
        b.sk_date,
        b.sk_hist_datetime AS atual_arrival_time,
        ts.expected_arrival_time,
        ts.expected_departure_time,
        ts.stop_pickup_type,
        ts.stop_drop_off_type,
        ts.stop_sequence,
        b.current_status,
        ts.distance_traveled,
        CASE
            WHEN EXTRACT(HOUR FROM b.sk_hist_datetime) <= CAST(LEFT(ts.expected_arrival_time, 2) AS INT64)
                 AND EXTRACT(MINUTE FROM b.sk_hist_datetime) <= CAST(RIGHT(LEFT(ts.expected_arrival_time, 5), 2) AS INT64)
            THEN "ON TIME"
            ELSE "LATE"
        END AS arrival_on_time, 
        current_timestamp AS updated_at
    FROM (
        SELECT DISTINCT *
        FROM {{ ref('stg_historical_stop_times') }} WHERE current_status = 'STOPPED_AT' 
    ) b
    LEFT JOIN trip_service ts
        ON CAST(ts.stop_id AS INT) = b.sk_stop
        AND ts.trip_id = b.sk_trip
)
SELECT *
FROM trip_status