with source as (select * from {{ source("spotify", "album_artists") }})

select album_id, artist_id
from source
