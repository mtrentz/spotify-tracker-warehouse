with

    artists as (select * from {{ ref("artists") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    all_album_artists as (select * from {{ ref("intermediate__all_album_artists") }})

select
    al.*,
    {{ generate_superset_html_album_card("al.album_name", "al.album_image_sm") }}
    as html_album_card,
    ar.artist_name as album_main_artist_name,
    al.album_name || ' - ' || ar.artist_name as album_and_artist_combined,
    aaa.artists as all_album_artists

from albums al
left join artists ar on al.album_main_artist_id = ar.artist_id
left join all_album_artists aaa on al.album_id = aaa.album_id
