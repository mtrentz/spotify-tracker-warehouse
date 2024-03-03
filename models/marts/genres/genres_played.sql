{{ config(materialized="incremental") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("tracks") }}),

    artist_genres as (select * from {{ ref("stg__artist_genres") }}),

    genres as (select * from {{ ref("genres") }}),

    amt_genres as (select * from {{ ref("intermediate__artists_amount_genres") }})

select
    sh.played_at,
    sh.ms_played,
    sh.minutes_played,
    sh.hours_played,

    -- Dividne by the amount of genres
    -- so I get a weighted time played
    sh.ms_played / greatest(amg.amount_genres, 1) as weighted_ms_played,
    sh.minutes_played / greatest(amg.amount_genres, 1) as weighted_minutes_played,
    sh.hours_played / greatest(amg.amount_genres, 1) as weighted_hours_played,

    t.*,
    g.genre_name,
    g.master_genre,
    g.master_genre_verbose

from streaming_history sh
left join tracks t on sh.track_id = t.track_id
-- Gets all artist genres (will duplicate rows)
left join artist_genres ag on t.artist_id = ag.artist_id
left join genres g on ag.genre_id = g.genre_id
left join amt_genres amg on ag.artist_id = amg.artist_id

{% if is_incremental() %}
    where sh.played_at > (select max(played_at) from {{ this }})
{% endif %}
