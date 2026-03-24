{{ config(
    materialized='table',
    cluster_by=['user_id', 'isbn']
) }}

select 
    user_id,
    isbn,
    book_rating,
    rating_sentiment,
    ingested_at
from {{ ref('int_ratings') }}