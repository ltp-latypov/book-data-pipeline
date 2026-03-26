{{ config(
   materialized='table',
   cluster_by=['author', 'country']
) }}
with base as (
   select
       -- Rating
       r.user_id,
       r.isbn,
       r.book_rating,
       r.rating_sentiment,
       r.ingested_at,
       -- Book
       b.title,
       b.author,
       b.publisher,
       b.release_year,
       b.book_age_category,
       b.book_format,
       b.book_poster,
       -- User
       u.age_group,
       u.country,
       u.city,
       u.is_full_profile
   from {{ ref('fct_ratings') }} r
   left join {{ ref('dim_books') }} b
       on r.isbn = b.isbn
   left join {{ ref('dim_users') }} u
       on r.user_id = u.user_id
),
-- 📊 1. Агрегація по країнах
aggregated_country as (
   select
       isbn,
       country,
       count(*) as ratings_count_country,
       avg(book_rating) as avg_rating_country
   from base
   group by isbn, country
),
-- 🌍 2. Глобальна агрегація
aggregated_global as (
   select
       isbn,
       count(*) as ratings_count_global,
       avg(book_rating) as avg_rating_global
   from base
   group by isbn
),
final as (
   select
       b.*,
       -- 📊 Country metrics
       ac.ratings_count_country,
       ac.avg_rating_country,
       -- 🌍 Global metrics
       ag.ratings_count_global,
       ag.avg_rating_global,
       -- ⭐ Popularity (global)
       case
           when ag.ratings_count_global >= 100 then 'Popular'
           when ag.ratings_count_global >= 20 then 'Moderate'
           else 'Niche'
       end as popularity_category,
       -- ⭐ Rating quality (global)
       case
           when ag.avg_rating_global >= 8 then 'Excellent'
           when ag.avg_rating_global >= 6 then 'Good'
           when ag.avg_rating_global >= 4 then 'Average'
           else 'Low Rated'
       end as avg_rating_category,
       -- 🧠 Window metrics (додатковий рівень аналітики)
       avg(b.book_rating) over (partition by b.author) as author_avg_rating,
       count(*) over (partition by b.author) as author_total_ratings,
       avg(b.book_rating) over (partition by b.country) as country_avg_rating,
       count(*) over (partition by b.country) as country_total_ratings
   from base b
   left join aggregated_country ac
       on b.isbn = ac.isbn
       and b.country = ac.country
   left join aggregated_global ag
       on b.isbn = ag.isbn
)
select * from final