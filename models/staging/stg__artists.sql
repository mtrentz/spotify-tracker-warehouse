with

    source as (select * from {{ source("spotify", "artists") }}),

    renamed as (
        select
            id as artist_id,
            name as artist_name,
            popularity as artist_popularity,
            followers as artist_followers
        from source
    )

select *
from renamed
