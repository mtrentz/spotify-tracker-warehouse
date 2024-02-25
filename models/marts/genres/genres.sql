with

    genres as (select * from {{ ref("stg__genres") }}),

    master_genres as (select * from {{ ref("seed__master_genres") }})

select genres.genre_id, genres.genre_name, master_genres.master_genre

from genres
left join master_genres on genres.genre_name = master_genres.genre
