with

    artists as (select * from {{ ref("stg__artists") }}),

    albums as (select * from {{ ref("stg__albums") }})

select al.*, ar.artist_name as album_main_artist_name

from albums al
left join artists ar on al.album_main_artist_id = ar.artist_id
