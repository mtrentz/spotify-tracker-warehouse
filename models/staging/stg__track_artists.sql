with source as (select * from {{ source("spotify", "track_artists") }})

select track_id, artist_id
from source
