with

    artists as (select * from {{ ref("artists") }}),

    artist_genres as (select * from {{ ref("stg__artist_genres") }}),

    genres as (select * from {{ ref("genres") }})

select
    artists.*,
    genres.genre_id,
    genres.genre_name,
    genres.master_genre,
    genres.master_genre_verbose

from artists
inner join artist_genres on artists.artist_id = artist_genres.artist_id
left join genres on artist_genres.genre_id = genres.genre_id
