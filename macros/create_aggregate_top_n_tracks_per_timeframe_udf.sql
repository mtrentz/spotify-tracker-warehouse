{% macro create_aggregate_top_n_tracks_per_timeframe_udf() %}

    create or replace function
        {{ target.schema }}.aggregate_top_n_tracks_per_timeframe(
            n integer, timeframe varchar
        )
    returns
        table(
            period timestamp,
            track_name varchar,
            artist_name varchar,
            all_track_artists varchar,
            total_ms_played bigint,
            total_minutes_played numeric(18, 2),
            total_hours_played numeric(18, 2)
        )
    language sql
    as $$

-- {% set postgres_time_unit = map_superset_time_grain_to_postgres_unit(time_grain) %}

with
    streaming_history as (select * from {{ ref("streaming_history") }}),

    timeframe_track_plays as (
        select
            date_trunc('{{ postgres_time_unit }}', played_at) as period,
            track_name,
            artist_name,
            all_track_artists,
            sum(ms_played) as total_ms_played
        from streaming_history
        group by period, track_name, artist_name, all_track_artists
    ),
    ranked_tracks as (
        select
            period,
            track_name,
            artist_name,
            all_track_artists,
            total_ms_played,
            rank() over (
                partition by period order by total_ms_played desc
            ) as track_rank
        from timeframe_track_plays
    ),
    top_tracks as (
        select period, track_name, artist_name, all_track_artists, total_ms_played
        from ranked_tracks
        where track_rank <= n
    ),
    tracks_with_others as (
        select period, track_name, artist_name, all_track_artists, total_ms_played
        from top_tracks
        union all
        select
            period,
            'Other' as track_name,
            null as artist_name,
            null as all_track_artists,
            sum(total_ms_played) as total_ms_played
        from ranked_tracks
        where track_rank > n
        group by period
    )
select
    period,
    track_name,
    artist_name,
    all_track_artists,
    sum(total_ms_played) as total_ms_played,
    sum(total_ms_played) / 60000 as total_minutes_played,
    sum(total_ms_played) / 3600000 as total_hours_played
from tracks_with_others
group by period, track_name, artist_name, all_track_artists
order by period, total_ms_played desc

$$

{% endmacro %}
