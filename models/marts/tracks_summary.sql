with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    track_stats as (select * from {{ ref("intermediate__track_stats") }}),

    all_track_artists as (select * from {{ ref("intermediate__all_track_artists") }}),

    all_album_artists as (select * from {{ ref("intermediate__all_album_artists") }})

select
    ts.track_id,
    ts.times_played,
    ts.total_ms_played,
    ts.total_minutes_played,
    ts.total_hours_played,
    ts.first_played_at,
    ts.last_played_at,
    ts.times_skipped,
    ts.skip_rate,
    ts.manual_plays,
    t.track_name,
    t.track_disc_number,
    t.track_duration_ms,
    t.track_is_explicit,
    t.track_popularity,
    t.track_track_number,
    t.track_is_local,
    ata.artists as all_track_artists,
    al.album_id,
    al.album_name,
    al.album_label,
    al.album_popularity,
    al.album_release_date,
    al.album_total_tracks,
    al.album_image_sm,
    aal.artists as all_album_artists,
    ar.artist_id,
    ar.artist_name,
    ar.artist_popularity,
    ar.artist_followers

from track_stats ts
left join tracks t on ts.track_id = t.track_id
left join albums al on t.track_album_id = al.album_id
left join artists ar on t.track_main_artist_id = ar.artist_id
left join all_track_artists ata on ts.track_id = ata.track_id
left join all_album_artists aal on al.album_id = aal.album_id
