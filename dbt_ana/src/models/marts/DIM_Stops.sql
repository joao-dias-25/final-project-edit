{{
    config(
        materialized = "table"
    )
}}

SELECT
    pk_stop,
    stop_id,
    stop_name,
    stop_short_name,
    stop_latitude,
    stop_longitude,
    region_id,
    region_name,
    district_id,
    district_name,
    municipality_id,
    municipality_name,
    locality,
    operational_status,
    near_school,
    near_airport,
    near_subway,
    near_train,
    CASE WHEN dbt_valid_to is null THEN 'Active' ELSE 'Inactive' END AS is_valid,
    dbt_updated_at as updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
    FROM {{ ref('DIM_Stops_snapshot') }}
