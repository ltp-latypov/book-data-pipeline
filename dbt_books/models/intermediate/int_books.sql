with stg_books as (
    select * from {{ ref('stg_books') }}
),

author_mapping as (
    select * from {{ ref('author_mappings') }}
),

joined as (
    select
        b.isbn,
        b.title,
        coalesce(m.canonical_author, b.author) as author,
        b.publisher,
        b.release_year,
        b.book_poster,
        b.ingested_at
    from stg_books b
    left join author_mapping m 
        on b.author = m.raw_author
),

age_calculations as (
    select
        *,
        extract(year from current_date()) - release_year as years_since_release
    from joined
)

select
    *,
    case 
        when years_since_release is null then 'Unknown'
        when years_since_release <= 2 then 'New Release'
        when years_since_release <= 10 then 'Contemporary'
        when years_since_release <= 30 then 'Modern'
        when years_since_release <= 100 then 'Vintage'
        else 'Antique/Classic'
    end as book_age_category, 

    case 
        when lower(title) like '%anthology%' or lower(title) like '%collection%' then 'Collection/Anthology'
        when regexp_contains(lower(title), r'\b(vol\.|volume|book|part)\b') then 'Series/Volume'
        when lower(title) like '%edition%' then 'Special Edition'
        else 'Standard Format'
    end as book_format

from age_calculations