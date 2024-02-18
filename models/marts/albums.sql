with

    artists as (select * from {{ ref("stg__artists") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    all_album_artists as (select * from {{ ref("intermediate__all_album_artists") }})

select al.*, ar.artist_name as album_main_artist_name, aaa.artists as all_album_artists

from albums al
left join artists ar on al.album_main_artist_id = ar.artist_id
left join all_album_artists aaa on al.album_id = aaa.album_id
