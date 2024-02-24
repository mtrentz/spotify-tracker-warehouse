{{ config(materialized="ephemeral") }}


with streaming_history as (select * from {{ ref("streaming_history") }})

select

    sh.album_and_artist_combined,
    count(sh.album_and_artist_combined) as times_played,
    -- amount of tracks of that album listened to
    count(distinct sh.track_id) as unique_tracks_from_album_played,
    sum(sh.ms_played) as total_ms_played,
    sum(sh.minutes_played) as total_minutes_played,
    sum(sh.hours_played) as total_hours_played,
    min(sh.played_at) as first_played_at,
    max(sh.played_at) as last_played_at

from streaming_history sh
-- I will be using this by "Album Name - Artist Name" so I will also group it!
-- If there are two occurances of the same album by the same artist, it will be
-- counted as one!
group by album_and_artist_combined
