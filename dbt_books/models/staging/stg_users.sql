select
    user_id,
    safe_cast(age as INT64) as age,
    country,
    city,
    region,
    location as full_location,
    ingested_at
from {{ source('books_cleaned', 'users') }}