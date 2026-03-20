select
    user_id,
    ISBN,
    safe_cast(book_rating as INT64) as book_rating,
    ingested_at
from {{ source('books_cleaned', 'ratings') }}