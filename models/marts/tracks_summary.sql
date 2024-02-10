with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    track_stats as (
        select
            sh.track_id,
            count(sh.track_id) as times_played,
            sum(sh.ms_played) as total_ms_played,
            sum(sh.minutes_played) as total_minutes_played,
            sum(sh.hours_played) as total_hours_played,
            min(sh.played_at) as first_played_at,
            max(sh.played_at) as last_played_at
        from streaming_history sh
        group by sh.track_id
    )

select
    ts.track_id,
    ts.times_played,
    ts.total_ms_played,
    ts.total_minutes_played,
    ts.total_hours_played,
    ts.first_played_at,
    ts.last_played_at,
    t.track_name,
    t.track_disc_number,
    t.track_duration,
    t.track_is_explicit,
    t.track_popularity,
    t.track_track_number,
    t.track_is_local,
    al.album_id,
    al.album_name,
    al.album_label,
    al.album_popularity,
    al.album_release_date,
    al.album_total_tracks,
    ar.artist_id,
    ar.artist_name,
    ar.artist_popularity,
    ar.artist_followers
from track_stats ts
left join tracks t on ts.track_id = t.track_id
left join albums al on t.track_album_id = al.album_id
left join artists ar on t.track_main_artist_id = ar.artist_id
