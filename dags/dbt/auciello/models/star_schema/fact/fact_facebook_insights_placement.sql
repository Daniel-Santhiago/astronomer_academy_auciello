{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_facebook_insights_placement'
    )
}}

select
  metric_date,
  account_name,
  campaign_name,
  adset_name,
  ad_name,
  publisher_platform,
  platform_position,
  sum(impressions)            as impressions,
  sum(video_view)             as video_view,
  sum(video_watch_25)         as video_watch_25,
  sum(video_watch_50)         as video_watch_50,
  sum(video_watch_75)         as video_watch_75,
  sum(video_watch_100)        as video_watch_100,
  sum(clicks)                 as clicks,
  sum(cost)                   as cost,
  sum(add_to_cart)            as add_to_cart,
  sum(initiate_checkout)      as initiate_checkout,
  sum(purchase)               as purchase,
  sum(purchase_value)         as purchase_value
from
  {{ ref('fact_facebook_insights_device') }}
group by
  all