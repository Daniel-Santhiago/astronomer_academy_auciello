{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_session_old',
        partition_by={
          "field": "session_datetime",
          "data_type": "datetime",
          "granularity": "day"
        },
        enabled=False
    )
}}

select
  --- session
  session_key                           as session_id,
  event_key                             as event_id,
  ga_session_id                         as ga_session_id,

  
  base_datetime                         as session_datetime,
  base_date                             as session_date,
  session_number,
  session_engagement_time_msec/1000.0   as session_engagement_time_sec,
  event_name,
  --- geo
  geo_country,
  geo_region,
  geo_city,
  --- page
  page_location,
  page_referrer,
  page_path,
  page_title,
  --- traffic
  canal                                 as channel,
  trafego                               as traffic_name,
  source,
  medium,
  campaign                              as campaign_name,
  content,
  term,
  gclid,
  fbclid,
  --- user
  user_id,
  user_pseudo_id,
  --- device
  platform,
  device_category,
  device_mobile_brand_name,
  device_mobile_model_name,
  device_mobile_marketing_name,
  device_operating_system,
  --- order
  replace(ecommerce_transaction_id,'T_','')   as tray_order_id,
  replace(item_id,'SKU_','')                  as tray_product_id,
  item_name                                   as tray_product_name,
  quantity                                    as tray_product_quantity,
  price                                       as tray_product_original_price,
  coupon,
  
  -- *
from
  -- `auciello-design.intermediate.ga4_enhanced_events` 
  {{ source('intermediate','enhanced_events') }}
where
  1=1
  and page_location not like '%https://gtm%'
  -- and base_date >= '2024-01-01'
  -- and base_date = "2024-03-17"