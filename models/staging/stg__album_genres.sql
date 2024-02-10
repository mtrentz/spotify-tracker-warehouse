with source as (select * from {{ source("spotify", "album_genres") }})

select album_id, genre_id
from source
