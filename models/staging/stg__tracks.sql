with

    source as (select * from {{ source("spotify", "tracks") }}),

    renamed as (
        select
            id as track_id,
            name as track_name,
            disc_number as track_disc_number,
            duration as track_duration,
            is_explicit as track_is_explicit,
            popularity as track_popularity,
            track_number as track_track_number,
            is_local as track_is_local,
            album_id as track_album_id,
            main_artist_id as track_main_artist_id
        from source
    )

select *
from renamed
