{{ config(materialized="ephemeral") }}


with
    artists as (select * from {{ ref("stg__artists") }}),

    albums as (select * from {{ ref("stg__albums") }}),

    album_artists as (select * from {{ ref("stg__album_artists") }})

select al.album_id, string_agg(a.artist_name, ', ') as artists

from albums al
left join album_artists aa on al.album_id = aa.album_id
left join artists a on aa.artist_id = a.artist_id
group by al.album_id
