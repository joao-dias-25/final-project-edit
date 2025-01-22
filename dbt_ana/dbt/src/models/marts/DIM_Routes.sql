{{
    config(
        materialized = "table"
    )
}}

WITH base_routes AS (
    SELECT 
        circular, 
        line_id, 
        line_long_name, 
        line_short_name, 
        line_type, 
        path_type,
        route_color,
        route_code, 
        route_long_name,
        route_short_name,
        route_text_color, 
        route_type,
        school,
        current_time AS current_time 
    FROM {{ ref('stg_Routes') }}
),

teste AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['route_code', 'current_time']) }} AS pk_route, 
        circular, 
        line_id, 
        line_long_name, 
        line_short_name, 
        line_type, 
        path_type,
        route_color,
        route_code, 
        route_long_name,
        route_short_name,
        route_text_color, 
        route_type,
        school,
        current_time, 
        1 AS is_valid, -- Alias válido
        CURRENT_TIMESTAMP() AS inserted_at, -- Alias válido
        SESSION_USER() AS inserted_by,      -- Alias válido
        CURRENT_TIMESTAMP() AS updated_at, -- Alias válido
        SESSION_USER() AS updated_by,      -- Alias válido
        GENERATE_UUID() AS uuid            -- Alias válido
    FROM base_routes
)

-- Listando explicitamente as colunas para evitar ambiguidade
SELECT 
    pk_route,
    circular, 
    line_id, 
    line_long_name, 
    line_short_name, 
    line_type, 
    path_type,
    route_color,
    route_code, 
    route_long_name,
    route_short_name,
    route_text_color, 
    route_type,
    school,
    current_time AS current_time ,
    is_valid,
    inserted_at,
    inserted_by,
    updated_at,
    updated_by,
    uuid
FROM teste
    
  