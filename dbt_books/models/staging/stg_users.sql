select
    safe_cast(user_id as int64) as user_id,
    
    case 
        when safe_cast(age as int64) between 5 and 100 
        then safe_cast(age as int64) 
        else null
    end as age,
    
    country,
    city,
    region,
    location as full_location,
    ingested_at
from {{ source('books_cleaned', 'users') }}