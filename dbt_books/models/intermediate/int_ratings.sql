{{ config(
    materialized='view'
) }}

with stg_ratings as (
    -- Reference the STAGING model
    select * from {{ ref('stg_ratings') }}
),

valid_books as (
    -- Reference the INTERMEDIATE books model
    select isbn from {{ ref('int_books') }}
),

valid_users as (
    -- Reference the INTERMEDIATE users model
    select user_id from {{ ref('int_users') }}
)

select
    r.user_id,
    r.isbn,
    r.book_rating,

    case 
        when r.book_rating <= 3 then 'Low'
        when r.book_rating <= 6 then 'Medium'
        when r.book_rating <= 8 then 'High'
        else 'Excellent'
    end as rating_sentiment,

    'Explicit' as rating_type,
    r.ingested_at

from stg_ratings r
-- INNER JOINs to enforce quality
inner join valid_books b on r.isbn = b.isbn
inner join valid_users u on r.user_id = u.user_id
where r.book_rating > 0