with

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    all_track_artists as (select * from {{ ref("intermediate__all_track_artists") }}),

    all_album_artists as (select * from {{ ref("intermediate__all_album_artists") }}),

    tracks_with_extended_details as (
        select
            t.*,
            ar.*,
            ata.artists as all_track_artists,
            al.*,
            aaa.artists as all_album_artists
        from tracks t
        left join albums al on t.track_album_id = al.album_id
        left join artists ar on t.track_main_artist_id = ar.artist_id
        left join all_track_artists ata on t.track_id = ata.track_id
        left join all_album_artists aaa on al.album_id = aaa.album_id
    )

select *
from tracks_with_extended_details
