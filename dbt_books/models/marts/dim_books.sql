{{ config(
    materialized='table',
    partition_by={
      "field": "release_year",
      "data_type": "int64",
      "range": {
        "start": 1300,
        "end": 2030,
        "interval": 10
      }
    },
    cluster_by=['author', 'book_age_category']
) }}

select 
    isbn,
    title,
    author,
    publisher,
    release_year,
    years_since_release,
    book_age_category,
    book_format,
    book_poster,
    ingested_at
from {{ ref('int_books') }}