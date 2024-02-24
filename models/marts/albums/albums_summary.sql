{{ config(materialized="table") }}

with

    albums as (select * from {{ ref("albums") }}),

    distinct_albums as (
        select distinct on (albums.album_and_artist_combined) albums.*
        from albums
        -- This is so that the most popular album (with the "correct" album art)
        -- is more likely placed on top!
        order by
            albums.album_and_artist_combined, coalesce(albums.album_popularity, 0) desc
    ),

    album_stats as (select * from {{ ref("intermediate__album_stats") }})

select
    als.times_played,
    als.unique_tracks_from_album_played,
    als.total_ms_played,
    als.total_minutes_played,
    als.total_hours_played,
    als.first_played_at,
    als.last_played_at,
    als.times_skipped,
    als.skip_rate,
    als.manual_plays,
    da.*
from album_stats als
left join
    distinct_albums da on da.album_and_artist_combined = als.album_and_artist_combined
