{{ 
    config(
        materialized = "table"
    ) 
}}

with agg_last_stop as (
    select 
        * 
    from 
        {{ ref('FACT_vehicles_trip_det') }}
    where 
        current_status = 'STOPPED_AT'
    qualify 
        row_number() over (
            partition by sk_trip, sk_vehicle, sk_date 
            order by sk_hist_datetime desc
        ) = 1
),

agg_first_stop as (
    select 
        *
    from 
        {{ ref('FACT_vehicles_trip_det') }}
    where 
        current_status = 'STOPPED_AT'
    qualify 
        row_number() over (
            partition by sk_trip, sk_vehicle, sk_date 
            order by sk_hist_datetime asc
        ) = 1
),

final as (
    select 
        sk_trip,
        sk_vehicle,
        sk_date,
        f.sk_line,
        f.sk_route,
        l.distance_traveled,
        f.sk_hist_datetime as departure_datetime,
        l.sk_hist_datetime as arrival_datetime,
        l.sk_hist_datetime - f.sk_hist_datetime as duration_trip,
        (TIMESTAMP_DIFF(l.sk_hist_datetime, f.sk_hist_datetime, SECOND) / 3600.0) as hour,
        SAFE_DIVIDE(
            cast(l.distance_traveled AS FLOAT64), 
            (TIMESTAMP_DIFF(l.sk_hist_datetime, f.sk_hist_datetime, SECOND) / 3600.0)
        ) as avg_speed,
        1 AS is_valid,
        CURRENT_TIMESTAMP() AS inserted_at,
        SESSION_USER()      AS inserted_by,
        CURRENT_TIMESTAMP() AS updated_at,
        SESSION_USER()      AS updated_by,
        GENERATE_UUID()     AS uuid
    from 
        agg_first_stop f
    left join 
        agg_last_stop l
    using 
        (sk_trip, sk_vehicle, sk_date)
)

select 
    *
from 
    final
