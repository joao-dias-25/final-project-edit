{{
    config(
        materialized = "view"
    )
}}

with routes as (
    select 
        case 
            when circular = '0' then 'NON CIRCULAR' 
            else 'CIRCULAR' 
        end as circular, 
        line_id, 
        line_long_name, 
        line_short_name, 
        line_type, 
        path_type,
        route_color,
        route_id as route_code, 
        route_long_name,
        route_short_name,
        route_text_color,
        case
            when route_type = '0' then 'Tram, Streetcar, Light rail'
            when route_type = '1' then 'Subway, Metro'
            when route_type = '2' then 'Rail'
            when route_type = '3' then 'Bus'
            when route_type = '4' then 'Ferry'
            when route_type = '5' then 'Cable tram'
            when route_type = '6' then 'Aerial lift, suspended cable car'
            when route_type = '7' then 'Funicular'
            when route_type = '11' then 'Trolleybus'
            when route_type = '12' then 'Monorail'
            else 'Unknown'
        end as route_type,
        school
    from {{ source('raw_data', 'staging_routes') }}
)

select *
from routes

