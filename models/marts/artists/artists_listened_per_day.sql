with streaming_history as (select * from {{ ref("streaming_history") }})

select

    date_trunc('day', played_at) as date,
    artist_id,
    artist_name,
    sum(minutes_played) as minutes_played,
    sum(sum(minutes_played)) over (
        partition by date_trunc('day', played_at)
    ) as daily_total_minutes_played

from streaming_history

group by 1, 2, 3
