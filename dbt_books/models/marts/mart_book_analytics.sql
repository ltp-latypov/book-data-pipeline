{{ config(
    materialized='table',
    cluster_by=['author', 'country']
) }}

with ratings as (
    select * from {{ ref('fct_ratings') }}
),

books as (
    select * from {{ ref('dim_books') }}
),

users as (
    select * from {{ ref('dim_users') }}
)

select
    -- Rating Data
    r.book_rating,
    r.rating_sentiment,

    -- Book Metadata
    b.isbn,
    b.title,
    b.author,
    b.publisher,
    b.release_year,
    b.book_age_category,
    b.book_format,
    b.book_poster,

    -- User Metadata
    u.user_id,
    u.age_group,
    u.country,
    u.city,
    u.is_full_profile,

    -- Timestamp
    r.ingested_at

from ratings r
-- Use LEFT JOIN here because we already did the INNER JOIN in int_ratings.
-- This ensures we keep all ratings in the fact table.
left join books b on r.isbn = b.isbn
left join users u on r.user_id = u.user_id