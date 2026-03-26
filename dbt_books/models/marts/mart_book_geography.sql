{{ config(materialized='table') }}

with books as (
    select * from {{ ref('stg_books') }}
),

ratings as (
    select * from {{ ref('stg_ratings') }}
),

users as (
    select * from {{ ref('int_users') }}
)

select
    b.title,
    b.author,
    u.country,
    u.city,
    r.book_rating,
    u.age
from ratings r
join books b on r.isbn = b.isbn
join users u on r.user_id = u.user_id