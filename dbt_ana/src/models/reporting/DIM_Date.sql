{{ 
    config(
        materialized = "table"
    ) 
}}


select s.* 
from {{ ref('DIM_Dates') }} s