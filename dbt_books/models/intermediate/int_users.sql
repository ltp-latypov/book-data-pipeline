with stg_users as (
    select * from {{ ref('stg_users') }}
)

select
    user_id,
    age,
    -- Create User Age Groups
    case 
        when age is null then 'Unknown'
        when age < 13 then 'Child (0-12)'
        when age < 20 then 'Teen (13-19)'
        when age < 40 then 'Young Adult (20-39)'
        when age < 60 then 'Adult (40-59)'
        else 'Senior (60+)'
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