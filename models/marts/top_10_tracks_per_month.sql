with
    streaming_history as (select * from {{ ref("streaming_history") }}),

    monthly_track_plays as (
        select
            date_trunc('month', played_at) as month,
            track_name,
            artist_name,
            all_track_artists,
            sum(ms_played) as total_ms_played
        from streaming_history
        group by month, track_name, artist_name, all_track_artists
    ),
    ranked_tracks as (
        select
            month,
            track_name,
            artist_name,
            all_track_artists,
            total_ms_played,
            rank() over (partition by month order by total_ms_played desc) as track_rank
        from monthly_track_plays
    ),
    top_tracks as (
        select month, track_name, artist_name, all_track_artists, total_ms_played
        from ranked_tracks
        where track_rank <= 10
    ),
    tracks_with_others as (
        select month, track_name, artist_name, all_track_artists, total_ms_played
        from top_tracks
        union all
        select
            month,
            'Other' as track_name,
            null as artist_name,  -- or 'Various Artists' if preferred
            null as all_track_artists,  -- or 'Various Artists'
            sum(total_ms_played) as total_ms_played
        from ranked_tracks
        where track_rank > 10
        group by month
    )
select
    month,
    track_name,
    artist_name,
    all_track_artists,
    sum(total_ms_played) as total_ms_played,
    sum(total_ms_played) / 60000 as total_minutes_played,
    sum(total_ms_played) / 3600000 as total_hours_played
from tracks_with_others
group by month, track_name, artist_name, all_track_artists
order by month, total_ms_played desc
