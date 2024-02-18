with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    album_stats as (select * from {{ ref("intermediate__album_stats") }})

select

    als.album_id,
    al.album_name,
    al.album_label,
    al.album_popularity,
    al.album_release_date,
    al.album_total_tracks,
    ar.artist_id,
    ar.artist_name,
    ar.artist_popularity,
    ar.artist_followers,
    als.times_played,
    als.unique_tracks_from_album_played,
    als.total_ms_played,
    als.total_minutes_played,
    als.total_hours_played,
    als.first_played_at,
    als.last_played_at

from album_stats als
left join albums al on als.album_id = al.album_id
-- Here I only care about the main artist of the album
left join artists ar on al.album_main_artist_id = ar.artist_id
