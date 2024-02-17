with
    repeated_tracks as (select * from {{ ref("intermediate__repeated_track_plays") }}),
    tracks as (select * from {{ ref("tracks") }})

select
    rt.start_time,
    rt.end_time,
    rt.repeats,
    rt.total_ms_played,
    rt.total_ms_played::float / 60000 as total_minutes_played,
    rt.total_ms_played::float / 3600000 as total_hours_played,

    t.*
from repeated_tracks rt
left join tracks t on rt.track_id = t.track_id
