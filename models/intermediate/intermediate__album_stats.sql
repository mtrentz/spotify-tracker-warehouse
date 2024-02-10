{{ config(materialized="ephemeral") }}


with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }})

select

    al.album_id,
    count(al.album_id) as times_played,
    -- amount of tracks of that album listened to
    count(distinct sh.track_id) as unique_tracks_from_album_played,
    sum(sh.ms_played) as total_ms_played,
    sum(sh.minutes_played) as total_minutes_played,
    sum(sh.hours_played) as total_hours_played,
    min(sh.played_at) as first_played_at,
    max(sh.played_at) as last_played_at

from streaming_history sh
left join tracks t on sh.track_id = t.track_id
left join albums al on t.track_album_id = al.album_id
group by al.album_id
