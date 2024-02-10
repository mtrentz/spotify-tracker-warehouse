with
    source as (select * from {{ source("spotify", "streaming_history") }}),

    renamed as (
        select
            played_at,
            ms_played,
            ms_played / 60000 as minutes_played,
            ms_played / 3600000 as hours_played,
            track_id,
            context,
            reason_start,
            reason_end,
            skipped as is_skipped,
            shuffle as is_shuffled
        from source
    )

select *
from renamed
