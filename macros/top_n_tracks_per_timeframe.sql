{% macro aggregate_top_n_tracks_per_timeframe(n, timeframe) %}

    with
        streaming_history as (select * from {{ ref("streaming_history") }}),

        timeframe_track_plays as (
            select
                date_trunc('{{ timeframe }}', played_at) as period,
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
            where track_rank <= {{ n }}
        ),
        tracks_with_others as (
            select period, track_name, artist_name, all_track_artists, total_ms_played
            from top_tracks
            union all
            select
                period,
                'Other' as track_name,
                null as artist_name,  -- or 'Various Artists' if preferred
                null as all_track_artists,  -- or 'Various Artists'
                sum(total_ms_played) as total_ms_played
            from ranked_tracks
            where track_rank > {{ n }}
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

{% endmacro %}