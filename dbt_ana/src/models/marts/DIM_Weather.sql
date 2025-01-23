{{
    config(
        materialized = "table"  
    )
}}

select * 
from {{ ref('stg_IPMA_weather') }}