{% snapshot DIM_Lines_snapshot %}
    {{
        config(
            target_schema='data_eng_project_group1_snapshots',
            unique_key='pk_line',
            strategy='check',
            check_cols = ["pk_line", "line_code", "long_name", "short_name", "color_line", "locality", "line_type"]
        )
    }}

WITH expanded_lines AS (
    SELECT
        l.line_code,
        l.long_name,
        l.short_name,
        l.color_line,
        l.lc,
        current_time AS current_time
    FROM {{ ref('stg_Lines') }} l
),
joined_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['el.line_code', 'current_time']) }} AS pk_line,  
        el.line_code,
        el.long_name,
        el.short_name,
        el.color_line,
        el.lc AS locality,
        r.line_type,
        1 AS is_valid,
        current_time AS current_time,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    FROM expanded_lines el
    LEFT JOIN {{ ref('stg_Routes') }} r
        ON el.line_code = r.line_id
)

SELECT *
FROM joined_data

{% endsnapshot %}