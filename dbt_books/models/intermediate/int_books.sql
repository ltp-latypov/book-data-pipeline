with base_data as (
    select * 
    from {{ ref('stg_books') }}
),

age_calculations as (
    select
        *,
        -- 2. Calculate "How many years since release"
        -- If year is null, age will also be null
        extract(year from current_date()) - release_year as years_since_release
    from base_data
)

select
    *,
    -- 3. Create Age Categories
    case 
        when years_since_release is null then 'Unknown'
        when years_since_release <= 2 then 'New Release (0-2 yrs)'
        when years_since_release <= 10 then 'Contemporary (3-10 yrs)'
        when years_since_release <= 30 then 'Modern (11-30 yrs)'
        when years_since_release <= 100 then 'Vintage (31-100 yrs)'
        else 'Antique/Classic (>100 yrs)'
    end as book_age_category
from age_calculations