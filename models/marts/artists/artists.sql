with artists as (select * from {{ ref("stg__artists") }})

select

    a.*,
    {{ generate_superset_html_artist_card("a.artist_name", "a.artist_image_sm") }}
    as html_artist_card

from artists a
