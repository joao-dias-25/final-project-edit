{{
    config(
        materialized = "view"
    )
}}

WITH lines AS (
    SELECT
        l.id AS line_code,
        l.long_name,
        l.short_name,
        l.color AS color_line,
        lc, -- localidade expandida
    FROM {{ source('raw_data', 'staging_carris_lines_data') }} l,
    UNNEST(l.localities) AS lc -- Expande a coluna localities em várias linhas
)

SELECT *
FROM lines
