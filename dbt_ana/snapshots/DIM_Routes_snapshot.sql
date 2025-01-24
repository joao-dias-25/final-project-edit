{% snapshot DIM_Routes_snapshot %}
    {{
        config(
            target_schema='data_eng_project_group1_snapshots',
            unique_key='pk_route',
            strategy='check',
            check_cols = ["route_long_name", "route_short_name", "route_color", "route_text_color", "circular", "route_type", "school"]
        )
    }}

    SELECT
        {{ dbt_utils.generate_surrogate_key(['route_code', 'current_time']) }} AS pk_route,
        route_code,
        route_long_name,
        route_short_name,
        route_color,
        route_text_color,
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
        END AS route_type,
        school,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER() AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER() AS updated_by,
        GENERATE_UUID() AS uuid
    FROM {{ ref('stg_Routes') }}

{% endsnapshot %}