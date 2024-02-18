with

    repeated_artists as (
        select * from {{ ref("intermediate__repeated_artist_plays") }}
    ),

    artists as (select * from {{ ref("artists") }})

select
    ra.start_time,
    ra.end_time,
    ra.repeats,
    ra.total_ms_played,
    -- total minutes and hours
    ra.total_ms_played::float / 60000 as total_minutes_played,
    ra.total_ms_played::float / 3600000 as total_hours_played,
    a.*
from repeated_artists ra
left join artists a on ra.artist_id = a.artist_id
