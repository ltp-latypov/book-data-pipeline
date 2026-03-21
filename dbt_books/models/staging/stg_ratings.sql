select
    safe_cast(user_id as int64) as user_id,
    TRIM(ISBN) as isbn,
    safe_cast(book_rating as int64) as book_rating,
    ingested_at
from {{ source('books_cleaned', 'ratings') }}