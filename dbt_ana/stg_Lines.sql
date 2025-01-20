/*
To reference this table onwards: {{ ref('stg_Lines') }}
*/

{{
    config(
        materialized = "view"
    )
}}

with lines as (
    select
        l.id as line_code,
        l.long_name,
        l.short_name,
        l.color as color_line,
        lc
    from {{ source('raw_data', 'staging_carris_lines_data') }} l
    cross join unnest(l.localities) as lc
)

select *
from lines
