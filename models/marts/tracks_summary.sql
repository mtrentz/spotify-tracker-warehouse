with
    streaming_history as (select * from {{ ref("stg__streaming_history") }}),

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    streaming_history_fully_joined as (

        select sh.played_at, sh.ms_played, t.track_id, al.album_id
        from streaming_history sh
        left join tracks t on sh.track_id = t.track_id
        left join albums al on t.track_album_id = al.album_id
        left join artists ar on t.track_main_artist_id = ar.artist_id

    )

select *
from streaming_history_fully_joined
