{{ config(materialized="incremental") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("tracks") }}),

    artist_genres as (select * from {{ ref("stg__artist_genres") }}),

    genres as (select * from {{ ref("genres") }})

select
    sh.played_at,
    sh.ms_played,
    sh.minutes_played,
    sh.hours_played,
    t.*,
    g.genre_name,
    g.master_genre

from streaming_history sh
inner join tracks t on sh.track_id = t.track_id
-- Gets all artist genres (will duplicate rows)
inner join artist_genres ag on t.artist_id = ag.artist_id
inner join genres g on ag.genre_id = g.genre_id

{% if is_incremental() %}
    where sh.played_at > (select max(played_at) from {{ this }})
{% endif %}
