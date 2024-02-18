with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("tracks") }}),

    albums as (select * from {{ ref("albums") }}),

    artists as (select * from {{ ref("artists") }}),

    track_stats as (select * from {{ ref("intermediate__track_stats") }}),

    all_track_artists as (select * from {{ ref("intermediate__all_track_artists") }})

select
    t.*,
    ts.times_played,
    ts.total_ms_played,
    ts.total_minutes_played,
    ts.total_hours_played,
    ts.first_played_at,
    ts.last_played_at,
    ts.times_skipped,
    ts.skip_rate,
    ts.manual_plays
from track_stats ts
left join tracks t on ts.track_id = t.track_id
left join albums al on t.track_album_id = al.album_id
left join artists ar on t.track_main_artist_id = ar.artist_id
left join all_track_artists ata on ts.track_id = ata.track_id
