with

    streaming_history as (select * from {{ ref("streaming_history") }}),

    monthly_artist_plays as (
        select
            date_trunc('month', played_at) as month,
            artist_name,
            sum(ms_played) as total_ms_played
        from streaming_history
        group by month, artist_name
    ),
    ranked_artists as (
        select
            month,
            artist_name,
            total_ms_played,
            rank() over (
                partition by month order by total_ms_played desc
            ) as artist_rank
        from monthly_artist_plays
    ),
    top_artists as (
        select month, artist_name, total_ms_played
        from ranked_artists
        where artist_rank <= 10
    ),
    artists_with_others as (
        select month, artist_name, total_ms_played
        from top_artists
        union all
        select month, 'Other' as artist_name, sum(total_ms_played) as total_ms_played
        from ranked_artists
        where artist_rank > 10
        group by month
    )
select
    month,
    artist_name,
    sum(total_ms_played) as total_ms_played,
    sum(total_ms_played) / 60000 as total_minutes_played,
    sum(total_ms_played) / 3600000 as total_hours_played
from artists_with_others
group by month, artist_name
order by month, total_ms_played desc
