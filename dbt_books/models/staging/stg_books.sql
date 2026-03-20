select
    ISBN,
    -- Basic cleanup for titles
    INITCAP(TRIM(title)) as title,
    
    -- THE FIX: 
    -- 1. TRIM extra spaces
    -- 2. REGEXP_REPLACE removes spaces after dots (J. R. R. -> J.R.R.)
    -- 3. INITCAP standardizes "MICHAEL CRICHTON" to "Michael Crichton"
    INITCAP(
        REGEXP_REPLACE(TRIM(author), r'\.\s+', '.')
    ) as author,
    
    -- Safe cast for the year to handle any remaining dirty data
    SAFE_CAST(year AS INT64) as release_year,
    
    publisher,
    ingested_at
from {{ source('books_cleaned', 'books') }}