with

    artists as (select * from {{ ref("artists") }}),

    artist_genres as (select * from {{ ref("stg__artist_genres") }})

select artists.artist_name, artists.artist_followers, artist_genres.*

from artists

left join artist_genres on artists.artist_id = artist_genres.artist_id

where artists.artist_name = 'John Wasson'
