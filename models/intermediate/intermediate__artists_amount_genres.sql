with artist_genres as (select * from {{ ref("artist_genres") }})

select
    artist_genres.artist_id, count(distinct artist_genres.genre_name) as amount_genres

from artist_genres

group by 1
