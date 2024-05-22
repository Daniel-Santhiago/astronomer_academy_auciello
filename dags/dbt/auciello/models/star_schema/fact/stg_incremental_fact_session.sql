
{{
    config(
        materialized = 'incremental',
        schema= 'star_schema',
        alias = 'stg_incremental_fact_session',
        incremental_strategy = 'insert_overwrite',
        unique_key = 'event_key',
        partition_by={
          "field": "base_date",
          "data_type": "date",
          "granularity": "day"
        },
        on_schema_change = 'fail',
        tags=['incremental', 'daily']
        
    )
}}

with source as (
    select 
        parse_date('%Y%m%d',event_date) as event_date_dt,

        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo,
        app_info,
        traffic_source,
        stream_id,
        platform,
        ecommerce,
        items,
      from {{ source('analytics', 'events_all') }}
        where
        1=1
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        --and substring(_table_suffix,-8)  >= cast(format_date('%Y%m%d',date_sub(current_date(), interval 3 day)) as string)

           
        AND
          (
            -- events from the 'intraday' tables should always be included
            _table_suffix LIKE 'intraday_%'
          OR 
            -- today, yesterday, or the day-before-yesterday
            PARSE_DATE('%Y%m%d', _table_suffix) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
            
          )

        {% endif %}


),
renamed as (
    select 
        DATETIME(TIMESTAMP_MICROS(event_timestamp),"America/Sao_Paulo") as base_datetime,
        DATE(TIMESTAMP_MICROS(event_timestamp),"America/Sao_Paulo") as base_date,
        event_date_dt,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, 
            CAST((select value.int_value from unnest(event_params) where key = 'ga_session_id') as STRING)
        ))) as session_key,



        
        event_timestamp,
        lower(replace(trim(event_name), " ", "_")) as event_name, -- Clean up all event names to be snake cased
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info.analytics_storage as privacy_info_analytics_storage,
        privacy_info.ads_storage as privacy_info_ads_storage,
        privacy_info.uses_transient_token as privacy_info_uses_transient_token,
        user_properties,
        user_first_touch_timestamp,
        user_ltv.revenue as user_ltv_revenue,
        user_ltv.currency as user_ltv_currency,
        device.category as device_category,
        device.mobile_brand_name as device_mobile_brand_name,
        device.mobile_model_name as device_mobile_model_name,
        device.mobile_marketing_name as device_mobile_marketing_name,
        device.mobile_os_hardware_model as device_mobile_os_hardware_model,
        device.operating_system as device_operating_system,
        device.operating_system_version as device_operating_system_version,
        device.vendor_id as device_vendor_id,
        device.advertising_id as device_advertising_id,
        device.language as device_language,
        device.is_limited_ad_tracking as device_is_limited_ad_tracking,
        device.time_zone_offset_seconds as device_time_zone_offset_seconds,
        device.browser as device_browser,
        device.browser_version as device_browser_version,
        device.web_info.browser as device_web_info_browser,
        device.web_info.browser_version as device_web_info_browser_version,
        device.web_info.hostname as device_web_info_hostname,
        geo.continent as geo_continent,
        geo.sub_continent as geo_sub_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.metro as geo_metro,
        app_info.id as app_info_id,
        app_info.version as app_info_version,
        app_info.install_store as app_info_install_store,
        app_info.firebase_app_id as app_info_firebase_app_id,
        app_info.install_source as app_info_install_source,
        traffic_source.name as campaign_user_level,
        traffic_source.medium as medium_user_level,
        traffic_source.source as source_user_level,
        stream_id,
        platform,
        ecommerce,
        items,
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "ga_session_id") as ga_session_id,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "page_location") as page_location,
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "ga_session_number") as session_number,
        (case when (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" then 1 end) as session_engaged,
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "engagement_time_msec") as engagement_time_msec,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "page_title") as page_title,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "page_referrer") as page_referrer,
        --(SELECT value.string_value FROM unnest(event_params) WHERE key = "source") as source_event_level,
        --(SELECT value.string_value FROM unnest(event_params) WHERE key = "medium") as medium_event_level,
        --(SELECT value.string_value FROM unnest(event_params) WHERE key = "campaign") as campaign_event_level,

        CASE 
          WHEN (SELECT LENGTH(value.string_value) FROM unnest(event_params) WHERE key = "gclid") > 0 
          THEN 'google'
          ELSE (select value.string_value from unnest(event_params) where key = 'source')
        END as source_event_level,
        CASE 
          WHEN (SELECT LENGTH(value.string_value) FROM unnest(event_params) WHERE key = "gclid") > 0 
          THEN 'cpc'
          ELSE (select value.string_value from unnest(event_params) where key = 'medium')
        END as medium_event_level,
        CASE 
          WHEN (SELECT LENGTH(value.string_value) FROM unnest(event_params) WHERE key = "gclid") > 0 
          THEN COALESCE(traffic_source.name,'(cpc)')
          ELSE (select value.string_value from unnest(event_params) where key = 'campaign')
        END as campaign_event_level,

        (SELECT value.string_value FROM unnest(event_params) WHERE key = "clarity_url") as clarity_url,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "content") as content,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "term") as term,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "search_term") as search_term,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "gclid") as gclid,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "fbclid") as fbclid,
        -- coalesce(
        --   (SELECT value.string_value FROM unnest(event_params) WHERE key = "fbclid"),
        --   REGEXP_EXTRACT(page_location, r'[?&]fbclid=([^&]*)'),
        --   REGEXP_EXTRACT(page_referrer, r'[?&]fbclid=([^&]*)')
        -- ) as fbclid,


        CASE 
            WHEN DATETIME(TIMESTAMP_MICROS(event_timestamp),"America/Sao_Paulo") >= '2023-03-12 22:55:00'
            THEN COALESCE((SELECT value.int_value FROM unnest(event_params) WHERE key = "search_results") ,0)
            ELSE COALESCE((SELECT value.int_value FROM unnest(event_params) WHERE key = "search_results") ,NULL)
        END  as search_results,
        
        -- CRIAÇÃO DE PARAMETROS CUSTOMIZADOS PARA CRIAÇÃO DE items
        (select cast(value.int_value as STRING) from unnest(event_params) where key = 'item_id') as item_id,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "item_name") as item_name,
        '' as item_brand,
        '' as item_variant,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "item_category") as item_category,
        '' as item_category2,
        '' as item_category3,
        '' as item_category4,
        '' as item_category5,
        CAST(0.0 AS FLOAT64) as price_in_usd,
        (select cast(value.double_value as FLOAT64) from unnest(event_params) where key = 'price') as price,
        (select cast(value.int_value as INT64) from unnest(event_params) where key = 'quantity') as quantity,
        --CASE 
        --WHEN event_name = 'buy_button' THEN 0 
        --WHEN event_name = 'add_to_cart' THEN 1
        --ELSE 
        --END as quantity,
        CAST(0.0 AS FLOAT64) as item_revenue_in_usd,
        (SELECT value.double_value FROM unnest(event_params) WHERE key = "item_revenue") as item_revenue,
        CAST(0.0 AS FLOAT64) as item_refund_in_usd,
        CAST(0.0 AS FLOAT64) as item_refund,
        (SELECT value.string_value FROM unnest(event_params) WHERE key = "coupon") as coupon,
        '' as affiliation,
        '' as location_id,
        '' as item_list_id,
        '' as item_list_name,
        '' as item_list_index,
        '' as promotion_id,
        '' as promotion_name,
        '' as creative_name,
        '' as creative_slot,
        
        CASE 
            WHEN event_name = 'page_view' THEN 1
            ELSE 0
        END AS is_page_view,
        CASE 
            WHEN event_name = 'purchase' THEN 1
            ELSE 0
        END AS is_purchase,
        CASE 
          WHEN  event_name = 'add_shipping_info'
          
          THEN 
            CASE 
              WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "value") IS NULL THEN  0.0
              WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "value") = '0.00' THEN 0.0
              WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "value") = 'null' THEN 0.0
              ELSE 
                CAST(
                  REPLACE(REPLACE(REPLACE(
                    (SELECT value.string_value FROM unnest(event_params) WHERE key = "value")
                    ,'R$ ',''),'.',''),',','.') 
                AS FLOAT64)
            END
            -- 0.0
        END as shipping_value,
    from source
)
, cte_analytics as (
  select         
  to_base64(md5(CONCAT(session_key, event_name, CAST(event_timestamp as STRING), to_json_string(event_params)))) as event_key, -- Surrogate key for unique events.  
  * from renamed
  qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1
)

, cte_events_items as (
    select event_key, 
      ARRAY_AGG(STRUCT(
      i.item_id as item_id,
          i.item_name as item_name,
          i.item_brand as item_brand,
          i.item_variant as item_variant,
          i.item_category as item_category,
          i.item_category2 as item_category2,
          i.item_category3 as item_category3,
          i.item_category4 as item_category4,
          i.item_category5 as item_category5,
          i.price_in_usd as price_in_usd,
          i.price as price,
          i.quantity as quantity,
          i.item_revenue_in_usd as item_revenue_in_usd,
          i.item_revenue as item_revenue,
          i.item_refund_in_usd as item_refund_in_usd,
          i.item_refund as item_refund,
          i.coupon as coupon,
          i.affiliation as affiliation,
          i.location_id as location_id,
          i.item_list_id as item_list_id,
          i.item_list_name as item_list_name,
          i.item_list_index as item_list_index,
          i.promotion_id as promotion_id,
          i.promotion_name as promotion_name,
          i.creative_name as creative_name,
          i.creative_slot  as creative_slot

      )) as items


  -- from cte_analytics
  from cte_analytics
  cross join unnest(items) i
  --where event_key= 'U/UOtwSJAMWGl4YHFR8sCw=='
  group by event_key

  
)
select 
a.* except(items), 
i.items as items
from cte_analytics a 
left join cte_events_items i using(event_key)

union all
select
  *
from
  `auciello-design.staging.ga4_base_events_manual_fix`