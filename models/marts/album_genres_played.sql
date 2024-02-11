{{ config(materialized="table") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    album_genres as (select * from {{ ref("stg__album_genres") }}),

    genres as (select * from {{ ref("stg__genres") }})

select
    sh.played_at,
    sh.ms_played,
    sh.minutes_played,
    sh.hours_played,
    sh.days_played,
    g.genre_name

from streaming_history sh
inner join tracks t on sh.track_id = t.track_id
inner join albums al on t.track_album_id = al.album_id
-- Gets all album genres (will duplicate rows)
inner join album_genres ag on al.album_id = ag.album_id
inner join genres g on ag.genre_id = g.genre_id
