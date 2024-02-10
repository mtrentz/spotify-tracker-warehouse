{{ config(materialized="ephemeral") }}


with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    track_artists as (select * from {{ ref("stg__track_artists") }})

select
    ar.artist_id,
    count(ar.artist_id) as times_played,
    count(distinct sh.track_id) as unique_tracks_played,
    count(distinct t.track_album_id) as unique_albums_played,
    sum(sh.ms_played) as total_ms_played,
    sum(sh.minutes_played) as total_minutes_played,
    sum(sh.hours_played) as total_hours_played,
    min(sh.played_at) as first_played_at,
    max(sh.played_at) as last_played_at
from streaming_history sh
-- Here I will use the m2m table to join tracks and artsits
-- which mean a track can count for minutes into multiple artists at once!
left join tracks t on sh.track_id = t.track_id
left join track_artists ta on t.track_id = ta.track_id
left join artists ar on ta.artist_id = ar.artist_id
group by ar.artist_id
