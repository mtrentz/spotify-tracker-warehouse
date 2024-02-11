{{ config(materialized="ephemeral") }}


with
    tracks as (select * from {{ ref("stg__tracks") }}),

    artists as (select * from {{ ref("stg__artists") }}),

    track_artists as (select * from {{ ref("stg__track_artists") }})

select t.track_id, string_agg(a.artist_name, ', ') as artists
from tracks t
left join track_artists ta on t.track_id = ta.track_id
left join artists a on ta.artist_id = a.artist_id
group by t.track_id
