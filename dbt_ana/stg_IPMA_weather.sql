{{
    config(
        materialized = "view"
    )
}}

select * from {{ source('raw_data', 'staging_ipma_lisbon_data') }} a