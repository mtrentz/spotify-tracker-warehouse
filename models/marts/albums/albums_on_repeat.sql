with

    repeated_albums as (select * from {{ ref("intermediate__repeated_album_plays") }}),

    albums as (select * from {{ ref("albums") }})

select
    ra.start_time,
    ra.end_time,
    ra.repeats,
    ra.total_ms_played,
    -- total minutes and hours
    ra.total_ms_played::float / 60000 as total_minutes_played,
    ra.total_ms_played::float / 3600000 as total_hours_played,
    a.*
from repeated_albums ra
left join albums a on ra.album_id = a.album_id
