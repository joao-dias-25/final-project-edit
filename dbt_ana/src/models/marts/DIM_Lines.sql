{{
    config(
        materialized = "table"
    )
}}

select 
    pk_line,  
    line_code,
    long_name,
    short_name,
    color_line,
    locality,
    line_type,
    CASE WHEN dbt_valid_to is null THEN 'Active' ELSE 'Inactive' END AS is_valid,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
from {{ ref('DIM_Lines_snapshot') }}
