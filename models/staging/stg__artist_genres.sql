with source as (select * from {{ source("spotify", "artist_genres") }})

select artist_id, genre_id
from source
