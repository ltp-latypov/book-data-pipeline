{{ config(materialized='table') }}

with books as (
    select * from {{ ref('stg_books') }}
),

ratings as (
    select * from {{ ref('stg_ratings') }}
)

select
    b.author,
    count(r.book_rating) as total_ratings,
    round(avg(r.book_rating), 2) as avg_rating,
    -- Also count how many unique books they have in our database
    count(distinct b.isbn) as unique_books_count
from ratings r
join books b on r.isbn = b.isbn
where r.book_rating > 0  -- 0 usually means "no rating" in this dataset
group by 1
having count(r.book_rating) >= 10
order by avg_rating desc