{{
    config(
        materialized = "table"
    )
}}

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
    CASE WHEN dbt_valid_to is null THEN 'Active' ELSE 'Inactive' END AS is_valid,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
FROM {{ ref('DIM_Routes_snapshot') }}
    
  