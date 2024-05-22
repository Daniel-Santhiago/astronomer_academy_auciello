{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_facebook_insights_device'
    )
}}

with
cte_ad_insights as (
  SELECT 
    metric_date,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    publisher_platform,
    platform_position,
    device_platform,
    impression_device,
    sum(impressions)            as impressions,
    sum(reach)                  as reach,
    sum(clicks)                 as clicks,
    sum(inline_link_clicks)     as inline_link_clicks,
    sum(spend)                  as spend,
  FROM 
    {{ source('kondado','facebook_ad_insights') }}
  group by
    all
)
, cte_ad_insights_actions as (
  SELECT 
    metric_date,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    publisher_platform,
    platform_position,
    device_platform,
    impression_device,
    sum(case when actions_action_type = 'video_view' then actions_value end)                                    as video_view,
    sum(case when actions_action_type = 'video_p25_watched_actions__video_view' then actions_value end)         as video_watch_25,
    sum(case when actions_action_type = 'video_p50_watched_actions__video_view' then actions_value end)         as video_watch_50,
    sum(case when actions_action_type = 'video_p75_watched_actions__video_view' then actions_value end)         as video_watch_75,
    sum(case when actions_action_type = 'video_p95_watched_actions__video_view' then actions_value end)         as video_watch_95,
    sum(case when actions_action_type = 'video_p100_watched_actions__video_view' then actions_value end)        as video_watch_100,
    sum(case when actions_action_type = 'link_click' then actions_value end)                                    as link_click,
    sum(case when actions_action_type = 'view_content' then actions_value end)                                  as view_content,
    sum(case when actions_action_type = 'landing_page_view' then actions_value end)                             as landing_page_view,
    sum(case when actions_action_type = 'add_to_cart' then actions_value end)       as add_to_cart,
    sum(case when actions_action_type = 'initiate_checkout' then actions_value end) as initiate_checkout,
    sum(case when actions_action_type = 'purchase' then actions_value end)          as purchase,
  FROM 
    {{ source('kondado','facebook_ad_insights_actions') }}
  group by
    all
)
, cte_ad_insights_action_values as (
  select 
    metric_date,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    publisher_platform,
    platform_position,
    device_platform,
    impression_device,
    -- sum(case when action_values_action_type = 'link_click' then actions_value end)        as link_click,
    -- sum(case when actions_action_type = 'add_to_cart' then actions_value end)       as add_to_cart,
    -- sum(case when actions_action_type = 'initiate_checkout' then actions_value end) as initiate_checkout,
    sum(case when action_values_action_type = 'purchase' then action_values_value end)          as purchase_value,
  from 
    {{ source('kondado','facebook_ad_insights_action_values') }}
  group by
    all
)
select 
  metric_date,
  account_name,
  campaign_name,
  adset_name,
  ad_name,
  publisher_platform,
  platform_position,
  device_platform,
  impression_device,
  impressions,
  reach,
  video_view,
  video_watch_25,
  video_watch_50,
  video_watch_75,
  video_watch_95,
  video_watch_100,
  view_content,
  landing_page_view,
  cte_ad_insights.inline_link_clicks    as clicks,
  spend                                 as cost,
  add_to_cart,
  initiate_checkout,
  purchase,
  cte_ad_insights_action_values.purchase_value,
from
  cte_ad_insights
left join
  cte_ad_insights_actions
using(metric_date,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    publisher_platform,
    platform_position,
    device_platform,
    impression_device)
left join
  cte_ad_insights_action_values
using(metric_date,
    account_name,
    campaign_name,
    adset_name,
    ad_name,
    publisher_platform,
    platform_position,
    device_platform,
    impression_device)
