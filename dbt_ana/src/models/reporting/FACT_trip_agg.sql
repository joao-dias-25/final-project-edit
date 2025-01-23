{{ 
    config(
        materialized = "table"
    ) 
}}

with agg_last_stop as (
Select * from (select 
                *, row_number() over (
                    partition by sk_trip, sk_vehicle, sk_date 
                    order by atual_arrival_time desc) as row_ 
                from 
                    {{ ref('FACT_vehicles_trip_det') }}
        )  where row_ = 1
),

agg_first_stop as (
 Select * from (select 
                *, row_number() over (
                    partition by sk_trip, sk_vehicle, sk_date 
                    order by atual_arrival_time asc) as row_ 
                from 
                    {{ ref('FACT_vehicles_trip_det') }}
        )  where row_ = 1
),

count_stops as (
    select sk_trip, 
            sk_vehicle, 
            sk_date, 
            count(sk_stop) as total_stops 
                from 
                    {{ ref('FACT_vehicles_trip_det') }}
            group by sk_trip, 
            sk_vehicle, 
            sk_date 
        ),

final as (
    select 
        f.sk_trip,
        f.sk_vehicle,
        f.sk_date,
        f.sk_line,
        f.sk_route,
        cast (l.distance_traveled as DECIMAL) as distance_traveled,
        f.atual_arrival_time as departure_datetime,
        l.atual_arrival_time as arrival_datetime,
        {{ datediff("f.atual_arrival_time","l.atual_arrival_time", "minute") }} as duration_trip_minute,
        {{ datediff("f.atual_arrival_time","l.atual_arrival_time", "minute") }} / 60 as duration_trip_hour,
        cs.total_stops,
        CURRENT_TIMESTAMP() AS updated_at,
    from 
        agg_first_stop f
    left join 
        agg_last_stop l on f.sk_trip=l.sk_trip and f.sk_vehicle=l.sk_vehicle and f.sk_date=l.sk_date
    left join 
        count_stops cs on f.sk_trip=cs.sk_trip and f.sk_vehicle=cs.sk_vehicle and f.sk_date=cs.sk_date 
)

select 
    * ,  distance_traveled / NULLIF(duration_trip_hour, 0) as avg_speed
from 
    final
