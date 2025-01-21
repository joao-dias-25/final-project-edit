{{
    config(
        materialized = "table"
    )
}}

WITH stops AS (
    SELECT 
        stop_id,
        stop_name,
        stop_short_name,
        stop_latitude,
        stop_longitude,
        region_id,
        region_name,
        district_id,
        district_name,
        municipality_id,
        municipality_name,
        locality,
        operational_status,
        near_school,
        near_airport,
        near_subway,
        near_train,
        current_time
    FROM {{ ref('stg_Stops') }}
),

surrogate_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stop_id', 'current_time']) }} AS pk_stop,  -- Gerando a chave substituta
        stop_id,
        stop_name,
        stop_short_name,
        stop_latitude,
        stop_longitude,
        region_id,
        region_name,
        district_id,
        district_name,
        municipality_id,
        municipality_name,
        locality,
        operational_status,
        near_school,
        near_airport,
        near_subway,
        near_train,
        current_time AS current_time,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    FROM stops
)

SELECT *
FROM surrogate_keys
