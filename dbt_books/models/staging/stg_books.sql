
select
    TRIM(ISBN) as isbn,
    title,
    INITCAP(
        REGEXP_REPLACE(TRIM(author), r'\.\s+', '.')
    ) as author,
    publisher,
    -- Apply the technical range check here
    case 
        when year < 1300 or year > extract(year from current_date())
        then null
        else year
    end as release_year,
    image_url_large as book_poster,
    ingested_at
from {{ source('books_cleaned', 'books') }}