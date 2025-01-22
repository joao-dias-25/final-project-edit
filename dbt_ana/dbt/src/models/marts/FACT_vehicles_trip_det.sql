{{ 
    config(
    materialized = "table"
) 
}}

WITH trip_status AS (
    SELECT DISTINCT
        CONCAT(b.sk_stop, '_', b.sk_trip, b.sk_date, b.current_status) AS pk_trip_stop_id,
        b.sk_stop,
        b.sk_trip,
        b.sk_vehicle,
        b.sk_line,
        t.sk_route,
        DATE(b.sk_date) as sk_date,
        b.sk_hist_datetime,
        b.current_status,
        a.distance_traveled,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    FROM 
        (SELECT DISTINCT * 
        FROM {{ ref('stg_historical_stop_times') }}) b
    LEFT JOIN (
        SELECT trip_id, stop_id, AVG(CAST(distance_traveled AS FLOAT64)) AS distance_traveled
        FROM {{ ref('stg_stop_times') }}
        GROUP BY trip_id, stop_id
    ) a
    ON a.trip_id = b.sk_trip
    AND CAST(a.stop_id AS STRING) = CAST(CONCAT('0', b.sk_stop) AS STRING)
    LEFT JOIN {{ ref('stg_trips') }} t
    ON b.sk_trip = t.trip_id
)

SELECT *
FROM trip_status
