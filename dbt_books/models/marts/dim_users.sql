{{ config(
    materialized='table',
    partition_by={
      "field": "age",
      "data_type": "int64",
      "range": {
        "start": 5,
        "end": 100,
        "interval": 1
      }
    },
    cluster_by=['country', 'age_group']
) }}

select 
    user_id,
    age,
    age_group,
    country,
    city,
    region,
    full_location,
    is_full_profile
from {{ ref('int_users') }}