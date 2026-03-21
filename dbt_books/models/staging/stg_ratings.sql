select
    user_id,
    TRIM(ISBN) as isbn,
    safe_cast(book_rating as INT64) as book_rating,
    ingested_at
from {{ source('books_cleaned', 'ratings') }}