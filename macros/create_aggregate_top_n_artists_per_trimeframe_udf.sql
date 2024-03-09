{% macro create_aggregate_top_n_artists_per_timeframe_udf() %}

    create or replace function
        {{ target.schema }}.aggregate_top_n_artists_per_timeframe(
            n integer, timeframe varchar
        )
    returns
        table(
            period timestamp,
            artist_name varchar,
            total_ms_played bigint,
            total_minutes_played numeric(18, 2),
            total_hours_played numeric(18, 2)
        )
    language sql
    as $$

with
    streaming_history as (select * from {{ ref("streaming_history") }}),

    timeframe_artist_plays as (
        select
            date_trunc(timeframe, played_at) as period,
            artist_name,
            sum(ms_played) as total_ms_played
        from streaming_history
        group by period, artist_name
    ),
    ranked_artists as (
        select
            period,
            artist_name,
            total_ms_played,
            rank() over (
                partition by period order by total_ms_played desc
            ) as artist_rank
        from timeframe_artist_plays
    ),
    top_artists as (
        select period, artist_name, total_ms_played
        from ranked_artists
        where artist_rank <= n
    ),
    artists_with_others as (
        select period, artist_name, total_ms_played
        from top_artists
        union all
        select
            period, 'Other' as artist_name, sum(total_ms_played) as total_ms_played
        from ranked_artists
        where artist_rank > n
        group by period
    )
select
    period,
    artist_name,
    sum(total_ms_played) as total_ms_played,
    sum(total_ms_played) / 60000 as total_minutes_played,
    sum(total_ms_played) / 3600000 as total_hours_played
from artists_with_others
group by period, artist_name
order by period, total_ms_played desc

$$

{% endmacro %}
