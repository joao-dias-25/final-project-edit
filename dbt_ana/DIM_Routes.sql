/*
To reference this table onwards: {{ ref('dim_route') }}
*/

{{
    config(
        materialized = "table"  
    )
}}

WITH base_routes AS (
    SELECT 
        route_id AS route_code, 
        route_long_name,
        route_short_name,
        route_color,
        route_text_color,
        school,
        CASE 
            WHEN circular = '0' THEN 'NON CIRCULAR' 
            ELSE 'CIRCULAR' 
        END AS circular,
        CASE 
            WHEN route_type = '0' THEN 'Tram'
            WHEN route_type = '1' THEN 'Subway'
            WHEN route_type = '2' THEN 'Rail'
            WHEN route_type = '3' THEN 'Bus'
            WHEN route_type = '4' THEN 'Ferry'
            WHEN route_type = '5' THEN 'Cable tram'
            WHEN route_type = '6' THEN 'Aerial Lift'
            WHEN route_type = '7' THEN 'Funicular'
            WHEN route_type = '11' THEN 'Trolleybus'
            ELSE 'Monorail'
        END AS route_type
    FROM {{ source('data_eng_project_group1', 'staging_routes') }}
),

dim_route AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key('route_id',  '1970-01-01')}} AS pk_routes,
        route_code,
        route_long_name,
        route_short_name,
        route_color,
        route_text_color,
        circular,
        route_type,
        school,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    FROM base_routes
)

SELECT *
FROM dim_route
