with

    source as (select * from {{ source("spotify", "genres") }}),

    renamed as (select id as genre_id, name as genre_name from source)

select *
from renamed
