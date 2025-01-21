
{{
    config(
        materialized = "table"  
    )
}}

WITH base_routes AS (
    SELECT 
        route_code, 
        route_long_name,
        route_short_name,
        route_color,
        route_text_color,
        school,
        circular,
        route_type,
        current_time AS current_time,
    FROM {{ ref('stg_Routes') }}
),

teste AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['route_code', 'current_time']) }} AS pk_route,  
        route_code,
        route_long_name,
        route_short_name,
        route_color,
        route_text_color,
        circular,
        route_type,
        school,
        current_time AS current_time,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    FROM base_routes
)

SELECT *
FROM teste
