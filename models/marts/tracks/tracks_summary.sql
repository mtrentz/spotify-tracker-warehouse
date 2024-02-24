{{ config(materialized="table") }}

with
    tracks as (select * from {{ ref("tracks") }}),

    -- Since I'm grouping by "track_name - artist_name" then when I'm joining
    -- it with the track table to get the rest of the information
    -- I just want to get the first one for that combination.
    distinct_tracks as (
        select distinct on (tracks.track_and_artist_combined) tracks.* from tracks
    ),

    track_stats as (select * from {{ ref("intermediate__track_stats") }})

select
    ts.times_played,
    ts.total_ms_played,
    ts.total_minutes_played,
    ts.total_hours_played,
    ts.first_played_at,
    ts.last_played_at,
    ts.times_skipped,
    ts.skip_rate,
    ts.manual_plays,
    dt.*
from track_stats ts
left join
    distinct_tracks dt on dt.track_and_artist_combined = ts.track_and_artist_combined
