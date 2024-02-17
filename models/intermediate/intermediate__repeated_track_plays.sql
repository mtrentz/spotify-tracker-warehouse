-- The objective here is to find out sequences of repeated songs played.
-- To do this I order the songs by played_at and then use the LAG function to
-- see the previous song played. If they were the same, I mark them as NULL.
-- Having these values as NULLs makes it easy to group them, similar to how you solve
-- a "forward fill" problem! This means I that for every sequence of NULLs I can make
-- a "group" like 1, 2, 3... 
-- After this I can just group by this "group" I created and count how many repeats
-- there were!
--
-- 2015-02-01T16:46:35	02Y78WneC8bUaKWouKl00v	23
-- 2015-02-01T16:46:35	0WF700uCpY2mQM5CUKuZ6H	24 <-- counts up whenever not repeated
-- 2015-02-01T16:46:36	02Y78WneC8bUaKWouKl00v	25 <-- repeats are always the same value
-- 2015-02-01T16:46:37	02Y78WneC8bUaKWouKl00v	25
-- 2015-02-01T16:46:37	0rSl8cEob8TpTItCqLPer1	26
{{ config(materialized="ephemeral") }}

with

    streaming_history as (select * from {{ ref("streaming_history") }}),

    repeated as (
        select
            played_at,
            track_id,
            ms_played,
            nullif(
                (lag(track_id) over (order by played_at, track_id) = track_id), true
            ) as repeated
        from streaming_history
        -- Filter only for when tracks were played to 50% or more
        where ms_played >= track_duration_ms / 2
        order by played_at, track_id
    ),

    repeated_groups as (
        select
            played_at,
            track_id,
            ms_played,
            count(repeated) over (order by played_at, track_id) as repeated_group
        from repeated
    ),

    repeated_sequences as (
        select
            track_id,
            min(played_at) as start_time,
            max(played_at) as end_time,
            sum(ms_played) as total_ms_played,
            count(*) as repeats
        from repeated_groups
        group by repeated_group, track_id
        having count(*) > 1
    )

select *
from repeated_sequences
