{{ config(materialized='table') }}

with

    artists as (select * from {{ ref("artists") }}),

    artist_stats as (select * from {{ ref("intermediate__artist_stats") }})

select
    ars.times_played,
    ars.total_ms_played,
    ars.total_minutes_played,
    ars.total_hours_played,
    ars.first_played_at,
    ars.last_played_at,
    ars.times_skipped,
    ars.skip_rate,
    ars.manual_plays,
    ar.*

from artist_stats ars
left join artists ar on ars.artist_id = ar.artist_id
