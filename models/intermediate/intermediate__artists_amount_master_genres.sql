with artist_master_genres as (select * from {{ ref("artist_master_genres") }})

select

    artist_master_genres.artist_id,
    count(distinct artist_master_genres.master_genre) as amount_master_genres

from artist_master_genres

group by 1
