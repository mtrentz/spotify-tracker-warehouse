{{ config(materialized="incremental") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    artist_genres as (select * from {{ ref("stg__artist_genres") }}),

    genres as (select * from {{ ref("stg__genres") }})

select sh.played_at, sh.ms_played, sh.minutes_played, sh.hours_played, g.genre_name

from streaming_history sh
inner join tracks t on sh.track_id = t.track_id
-- Will get genres only on the main artist of the track
inner join artists ar on t.track_main_artist_id = ar.artist_id
-- Gets all artist genres (will duplicate rows)
inner join artist_genres ag on ar.artist_id = ag.artist_id
inner join genres g on ag.genre_id = g.genre_id

{% if is_incremental() %}
    where sh.played_at > (select max(played_at) from {{ this }})
{% endif %}
