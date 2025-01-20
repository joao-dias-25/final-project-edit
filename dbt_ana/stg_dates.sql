/* 
Using dbt_date package -> get_date_dimension macro
from https://github.com/calogica/dbt-date/tree/0.10.1/#get_date_dimensionstart_date-end_date
As bonus added audit column ingested_at
*/
{{
    config(
        materialized = "view",
    )
}}


with dates as ({{ dbt_date.get_date_dimension("2015-01-01", "2050-12-31") }})

select dates.*, current_timestamp as ingested_at
from dates