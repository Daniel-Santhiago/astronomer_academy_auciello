{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_creative_insights_all_time_active'
    )
}}

select
  *
from
  {{ ref('az_facebook_creative_insights_all_time') }}
where
  1=1
  and ad_end_date >= current_date() - 1
