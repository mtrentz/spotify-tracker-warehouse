with

    tracks as (select * from {{ ref("stg__tracks") }}),

    albums as (select * from {{ ref("albums") }}),

    artists as (select * from {{ ref("artists") }}),

    all_track_artists as (select * from {{ ref("intermediate__all_track_artists") }}),

    tracks_with_extended_details as (
        select
            t.*,
            {{
                generate_superset_html_track_card(
                    "t.track_name", "ata.artists", "al.album_image_sm"
                )
            }} as html_track_card,
            ar.*,
            t.track_name || ' - ' || ar.artist_name as track_and_artist_combined,
            ata.artists as all_track_artists,
            al.*
        from tracks t
        left join albums al on t.track_album_id = al.album_id
        left join artists ar on t.track_main_artist_id = ar.artist_id
        left join all_track_artists ata on t.track_id = ata.track_id
    )

select *
from tracks_with_extended_details
