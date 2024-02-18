-- Same logic as intermediate__repeated_tracks.sql but for artists instead of tracks.
-- Check there for comments on the logic.
{{ config(materialized="ephemeral") }}

with
    streaming_history as (select * from {{ ref("streaming_history") }}),

    repeated as (
        select
            played_at,
            artist_id,
            ms_played,
            nullif(
                (lag(artist_id) over (order by played_at, artist_id) = artist_id), true
            ) as repeated
        from streaming_history
        where ms_played >= track_duration_ms / 2
        order by played_at, artist_id
    ),

    repeated_groups as (
        select
            played_at,
            artist_id,
            ms_played,
            count(repeated) over (order by played_at, artist_id) as repeated_group
        from repeated
    ),

    repeated_sequences as (
        select
            artist_id,
            min(played_at) as start_time,
            max(played_at) as end_time,
            sum(ms_played) as total_ms_played,
            count(*) as repeats
        from repeated_groups
        group by repeated_group, artist_id
        having count(*) > 1
    )

select *
from repeated_sequences
