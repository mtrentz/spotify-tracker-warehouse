with

    genres as (select * from {{ ref("stg__genres") }}),

    master_genres as (select * from {{ ref("seed__master_genres") }}),

    master_genres_verbose as (select * from {{ ref("seed__master_genres_verbose") }})

select
    genres.genre_id,
    genres.genre_name,
    master_genres.master_genre,

    coalesce(
        master_genres_verbose.master_genre_verbose, master_genres.master_genre
    ) as master_genre_verbose

from genres
left join master_genres on genres.genre_name = master_genres.genre
left join
    master_genres_verbose
    on master_genres.master_genre = master_genres_verbose.master_genre
