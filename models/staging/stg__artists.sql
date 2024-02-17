with

    source as (select * from {{ source("spotify", "artists") }}),

    renamed as (
        select
            id as artist_id,
            name as artist_name,
            popularity as artist_popularity,
            followers as artist_followers,
            image_sm as artist_image_sm
        from source
    )

select *
from renamed
