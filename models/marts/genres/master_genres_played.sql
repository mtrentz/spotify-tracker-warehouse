{{ config(materialized="incremental") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("tracks") }}),

    artist_master_genres as (select * from {{ ref("artist_master_genres") }}),

    amnt_master_genres as (
        select * from {{ ref("intermediate__artists_amount_master_genres") }}
    )

select
    sh.played_at,
    sh.ms_played,
    sh.minutes_played,
    sh.hours_played,

    -- Divide the amount played by the amount of master genres
    -- so I get a weighted time played
    sh.ms_played / greatest(amnt.amount_master_genres, 1) as weighted_ms_played,
    sh.minutes_played
    / greatest(amnt.amount_master_genres, 1) as weighted_minutes_played,
    sh.hours_played / greatest(amnt.amount_master_genres, 1) as weighted_hours_played,

    t.*,
    amg.master_genre,
    amg.master_genre_verbose

from streaming_history sh
left join tracks t on sh.track_id = t.track_id
left join artist_master_genres amg on t.artist_id = amg.artist_id
left join amnt_master_genres amnt on amg.artist_id = amnt.artist_id

{% if is_incremental() %}
    where sh.played_at > (select max(played_at) from {{ this }})
{% endif %}
