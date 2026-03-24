with stg_users as (
    select * from {{ ref('stg_users') }}
)

select
    user_id,
    age,
    -- Create User Age Groups
    case 
        when age is null then 'Unknown'
        when age < 13 then 'Child'
        when age < 20 then 'Teen'
        when age < 40 then 'Young Adult'
        when age < 60 then 'Adult'
        else 'Senior'
    end as age_group,    
    country,
    city,
    region,
    full_location,
    case 
        when age is not null and country != 'Unknown' and city != 'Unknown' then true
        else false
    end as is_full_profile,
    ingested_at
from stg_users