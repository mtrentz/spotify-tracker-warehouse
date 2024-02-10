with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    artist_stats as (select * from {{ ref("intermediate__artist_stats") }})

select

    ars.artist_id,
    ar.artist_name,
    ar.artist_popularity,
    ar.artist_followers,
    ars.times_played,
    ars.unique_tracks_played,
    ars.unique_albums_played,
    ars.total_ms_played,
    ars.total_minutes_played,
    ars.total_hours_played,
    ars.first_played_at,
    ars.last_played_at

from artist_stats ars
left join artists ar on ars.artist_id = ar.artist_id
