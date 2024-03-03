with

    artist_genres as (select * from {{ ref("stg__artist_genres") }}),
    artists as (select * from {{ ref("stg__artists") }}),

    genres as (select * from {{ ref("genres") }})

-- Get only the the artist distinct master genres
select distinct
    on (artist_genres.artist_id, genres.master_genre)
    artist_genres.artist_id,
    artists.artist_name,
    genres.master_genre,
    genres.master_genre_verbose

from artist_genres
inner join genres on artist_genres.genre_id = genres.genre_id
inner join artists on artist_genres.artist_id = artists.artist_id

where genres.master_genre is not null
