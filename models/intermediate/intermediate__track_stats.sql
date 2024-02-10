{{ config(materialized="ephemeral") }}

with streaming_history as (select * from {{ ref("stg__streaming_history") }})

select
    sh.track_id,
    count(sh.track_id) as times_played,
    sum(sh.ms_played) as total_ms_played,
    sum(sh.minutes_played) as total_minutes_played,
    sum(sh.hours_played) as total_hours_played,
    min(sh.played_at) as first_played_at,
    max(sh.played_at) as last_played_at
from streaming_history sh
group by sh.track_id
