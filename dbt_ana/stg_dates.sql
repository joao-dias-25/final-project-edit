
{{
    config(
        materialized = "view",
    )
}}


with dates as ({{ dbt_date.get_date_dimension("2015-01-01", "2050-12-31") }})

select dates.*, current_timestamp as ingested_at
from dates