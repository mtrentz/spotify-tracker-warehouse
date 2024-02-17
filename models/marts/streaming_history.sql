{{ config(materialized="materialized_view") }}

with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("tracks") }})

select
    sh.played_at,
    sh.ms_played,
    sh.minutes_played,
    sh.hours_played,
    sh.context,
    sh.reason_start,
    sh.reason_end,
    sh.is_skipped,
    sh.is_shuffled,
    t.*
from streaming_history sh
left join tracks t on sh.track_id = t.track_id
