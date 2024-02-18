with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    artists as (select * from {{ ref("artists") }}),

    artist_stats as (select * from {{ ref("intermediate__artist_stats") }})

select

    ar.*,
    ars.times_played,
    ars.unique_tracks_played,
    ars.unique_albums_played,
    ars.total_ms_played,
    ars.total_minutes_played,
    ars.total_hours_played,
    ars.first_played_at,
    ars.last_played_at,
    ars.times_skipped,
    ars.skip_rate,
    ars.manual_plays,
    ars.avg_track_listen_minutes

from artist_stats ars
left join artists ar on ars.artist_id = ar.artist_id
