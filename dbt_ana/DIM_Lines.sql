/*
To reference this table onwards: {{ ref('your_model_name') }}
*/

{{
    config(
        materialized = "view"  -- Altere para "table" se necessário
    )
}}

with 
lines as (
    select
        l.id as line_code,
        l.long_name,
        l.short_name,
        l.color as color_line,
        lc
    from {{ ref('stg_lines') }} l,
    unnest(l.localities) as lc
),
joined_data as (
    select
        {{ 
      dbt_utils.safe_cast('l.id', 'string') 
  }} || '1970-01-01' AS pk_line,
        l.line_code,
        l.long_name,
        l.short_name,
        l.color_line,
        l.lc,
        r.line_type
    from lines l
    left join {{ ref('stg_routes') }} r
        on l.line_code = r.line_id
)

select *
from joined_data
