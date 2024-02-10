with

    source as (select * from {{ source("spotify", "albums") }}),

    renamed as (
        select
            id as album_id,
            name as album_name,
            label as album_label,
            popularity as album_popularity,
            release_date as album_release_date,
            total_tracks as album_total_tracks,
            main_artist_id as album_main_artist_id
        from source
    )

select *
from renamed
