select
    TRIM(ISBN) as isbn,
    -- Basic cleanup for titles
    INITCAP(TRIM(title)) as book_title,
    
    -- THE FIX: 
    -- 1. TRIM extra spaces
    -- 2. REGEXP_REPLACE removes spaces after dots (J. R. R. -> J.R.R.)
    -- 3. INITCAP standardizes "MICHAEL CRICHTON" to "Michael Crichton"
    INITCAP(
        REGEXP_REPLACE(TRIM(author), r'\.\s+', '.')
    ) as author,
    
    -- Safe cast for the year to handle any remaining dirty data
    NULLIF(SAFE_CAST(year AS INT64), 0) as release_year,
    
    publisher,
    ingested_at
from {{ source('books_cleaned', 'books') }}