
{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_session',
        partition_by={
          "field": "session_datetime",
          "data_type": "datetime",
          "granularity": "day"
        },
    )
}}

with
traffic_events as (
  select 
    ts.session_key,
    ts.source,
    ts.medium,
    ts.campaign,
    ts.content,
    ts.term,
    ts.gclid,
    ts.fbclid,
    session.* except(session_key, content, term, gclid, fbclid)
  from 
    {{ ref('stg_incremental_fact_session')}} session
  left join 
    {{ ref('fact_traffic_session') }} ts
  on 
    session.session_key = ts.session_key 
)
, custom_events as (
  select 
    case 
      when event_name = 'click' and (SELECT value.string_value FROM unnest(event_params) WHERE key = "link_domain") = 'api.whatsapp.com' 
      then 'click_whatsapp'
      when event_name = 'click' and (SELECT value.string_value FROM unnest(event_params) WHERE key = "link_domain") = 'instagram.com' 
      then 'click_instagram'
      when event_name = 'click' and (SELECT value.string_value FROM unnest(event_params) WHERE key = "link_domain") = 'youtube.com' 
      then 'click_youtube'
      when event_name = 'click' and (SELECT value.string_value FROM unnest(event_params) WHERE key = "link_domain") = 'tiktok.com' 
      then 'click_tiktok'
      when event_name = 'click' and (SELECT value.string_value FROM unnest(event_params) WHERE key = "link_domain") = 'br.pinterest.com' 
      then 'click_pinterest'
      else event_name
    end as event_name,
    * except(event_name)
  from 
    traffic_events
)
, custom_items as (
    select 
      * except (items),
      CASE 
        WHEN event_name IN ('read_description','preview_shipping', 'buy_button', 'add_to_cart' ) 
        THEN ARRAY_AGG(
          STRUCT(
            coalesce(item_id, (select string_agg(item_id,',') from unnest(items)) ) as item_id,
            coalesce(item_name, (select string_agg(item_name,',') from unnest(items)) ) as item_name,
            item_brand,
            item_variant,
            item_category,
            item_category2,
            item_category3,
            item_category4,
            item_category5,
            price_in_usd,
            price,
            quantity,
            item_revenue_in_usd,
            item_revenue,
            item_refund_in_usd,
            item_refund,
            coupon,
            affiliation,
            location_id,
            item_list_id,
            item_list_name,
            item_list_index,
            promotion_id,
            promotion_name,
            creative_name,
            creative_slot
          )
        ) over 
        ( partition by session_key , event_name, event_timestamp)
        ELSE items 
      END items,
    from 
      custom_events
)
, flat_tbl as (
  select
    first_value(source IGNORE NULLS) over(partition by session_key order by event_date_dt) as source, 
    first_value(medium IGNORE NULLS) over(partition by session_key order by event_date_dt) as medium,
    first_value(campaign IGNORE NULLS) over(partition by session_key order by event_date_dt) as campaign,
    first_value(clarity_url IGNORE NULLS) over(partition by session_key order by event_date_dt) as clarity_url,
    first_value(content IGNORE NULLS) over(partition by session_key order by event_date_dt) as content,
    first_value(gclid IGNORE NULLS) over(partition by session_key order by event_date_dt) as gclid,

    first_value(geo_continent IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_continent,
    first_value(geo_sub_continent IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_sub_continent,
    first_value(geo_country IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_country,
    first_value(geo_region IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_region,
    first_value(geo_city IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_city,
    first_value(geo_metro IGNORE NULLS) over(partition by session_key order by event_date_dt) as geo_metro,
    
    --first_value(site_version IGNORE NULLS) over(partition by session_key order by event_date_dt) as site_version,
    --first_value(customer_id IGNORE NULLS) over(partition by session_key order by event_date_dt) as customer_id,
    --REGEXP_EXTRACT( page_location, '(?:\\\\w+:)?\\\\/\\\\/[^\\\\/]+([^?#]+)') as page_path,
    -- REGEXP_EXTRACT(page_location, r'https?://[^/]+(/.*)') AS page_path,
    REGEXP_EXTRACT(page_location, r'^https?://[^/]+(/[^?#]*)?') AS page_path,
    REGEXP_EXTRACT(page_location, r'order_id=([a-zA-Z0-9]+)') AS order_id,
    page_location,
    page_referrer,
    coalesce(engagement_time_msec, 0) as engagement_time_msec,
    * except(event_bundle_sequence_id,
            event_params, 
            source, 
            medium, 
            campaign, 
            clarity_url,
            content, 
            gclid,
            geo_continent,
            geo_sub_continent,
            geo_country,
            geo_region,
            geo_city,
            geo_metro,
            --site_version, 
            --customer_id, 
            page_location, page_referrer, engagement_time_msec,
            -- items_fields
            item_id,
            item_name,
            item_brand,
            item_variant,
            item_category,
            item_category2,
            item_category3,
            item_category4,
            item_category5,
            price_in_usd,
            price,
            quantity,
            item_revenue_in_usd,
            item_revenue,
            item_refund_in_usd,
            item_refund,
            --coupon,
            affiliation,
            location_id,
            item_list_id,
            item_list_name,
            item_list_index,
            promotion_id,
            promotion_name,
            creative_name,
            creative_slot
            )
  from 
    custom_items
  order by 
    base_datetime desc 
)
, flat_items_tbl as (
  select 
    event_key,
    event_name,
    base_datetime,
    i.item_id,
    i.item_name,
    i.item_brand,
    i.item_variant,
    i.item_category,
    i.item_category2,
    i.item_category3,
    i.item_category4,
    i.item_category5,
    i.price_in_usd,
    i.price,
    i.quantity,
    i.item_revenue_in_usd,
    CASE 
    WHEN event_name IN ('view_cart') THEN ROUND((i.price * quantity),2)
    ELSE i.item_revenue
    END item_revenue,
    i.item_refund_in_usd,
    i.item_refund,
    i.coupon,
    i.affiliation,
    i.location_id,
    i.item_list_id,
    i.item_list_name,
    i.item_list_index,
    i.promotion_id,
    i.promotion_name,
    i.creative_name,
    i.creative_slot
  from 
    flat_tbl
  cross join 
    unnest(items) as i
  where 
    event_name in (
    'add_payment_info', 
    'add_shipping_info', 
    'add_to_cart',
    'add_to_wishlist',
    'begin_checkout' ,
    'buy_button',
    'purchase',
    'refund', 
    'remove_from_cart',
    'select_item', 
    'select_promotion',
    'view_cart',
    'view_item_list',
    'view_promotion',
    'view_item',
    'read_description',
    'preview_shipping'
    )
  order by 
    base_datetime desc 
)
, dedup_events as (
  select 
    e.* except(items, coupon),
    i.* except(event_key, event_name, base_datetime, coupon),
    coalesce(e.coupon, i.coupon) as coupon
  from 
    flat_tbl e
  left join 
    flat_items_tbl i
  on 
    e.event_key = i.event_key
    and e.event_name = i.event_name
    and e.base_datetime  = i.base_datetime
)
, cte_clean_events as (
    select 
      e.item_name, 
      case 
          when e.event_name = 'purchase' 
            and orders.order_status = 'Cancelado' then 'purchase_cancelled'
          else e.event_name
      end event_name,
      sum(engagement_time_msec) over(partition by session_key) as session_engagement_time_msec,
      e.* except(item_name, event_name)
    from 
      dedup_events e
    left join 
        {{ ref('fact_order')}} orders
    on 
        replace(ecommerce.transaction_id,'T_','') = orders.tray_order_id


    QUALIFY 
    (event_timestamp - lag(event_timestamp, 1) over(partition by user_pseudo_id, event_name, ga_session_id, event_key, item_id order by event_timestamp) >= 1000000)
    or 
    (event_timestamp - lag(event_timestamp, 1) over(partition by user_pseudo_id, event_name, ga_session_id, event_key, item_id order by event_timestamp) is null )
    )
, cte_gads_click_stats as (
    select 
        DATE(cls._PARTITIONTIME) as base_date_click,
        cls.click_view_gclid as gclid, 
        cls.campaign_id,
        cls.segments_click_type,
        cmp.campaign_name,
        cmp.campaign_advertising_channel_type,
    from 
      {{ source('google_ads_preview','p_ads_ClickStats') }} cls
    left join 
      {{ source('google_ads_preview','p_ads_Campaign') }} cmp
    on 
      cls.campaign_id = cmp.campaign_id and DATE(cls._PARTITIONTIME) = DATE(cmp._PARTITIONTIME)
    where
      1=1 

)
, cte_enhanced_campaign as (
  select 
    e.* except(campaign),
    coalesce(g.campaign_name, e.campaign) as campaign, 
    g.campaign_advertising_channel_type as google_ads_campaign_type,
    g.segments_click_type as google_ads_click_type,
    row_number() over(partition by session_key order by base_datetime asc) as session_event_rank
  from 
    cte_clean_events e
  left join 
    cte_gads_click_stats g
  on 
    g.gclid = e.gclid
)
, cte_gclid_mapping as (
    select
      distinct
      gclid,
      case 
        --when event_name = 'session_start' and  google_ads_click_type = 'URL_CLICKS'
          when session_event_rank = 1 and  (google_ads_click_type IN ('URL_CLICKS','UNKNOWN') or google_ads_click_type is null)
          then 
              case 
                  when page_referrer like '%google.com%/' then 'Google Search'
                  when page_referrer like '%com.google%' then 'Google App'
                  when page_referrer like '%googleads.g.doubleclick.net%' then 'Display Google AdSense'
                  when page_referrer like '%youtube.com%' then 'Youtube'
                  when page_referrer like '%googlesyndication.com%' then 'Google Ad Manager'
                  when page_referrer like '%taboola%' then 'Taboola'
                  when page_referrer like '%auciellodesign%' then 'Auciello Design'
              end
        
        --when event_name = 'session_start' and  google_ads_click_type = 'PRODUCT_LISTING_AD_CLICKS'
          when session_event_rank = 1 and  google_ads_click_type = 'PRODUCT_LISTING_AD_CLICKS'
          then 'Google Shopping'
        
          else content
      end as content,
      page_path,

    from 
      cte_enhanced_campaign
    where 
      1=1
      and source = 'google' and medium = 'cpc'
      and session_event_rank = 1
)
, cte_traffic_attribution as (
  select 
    case 
        when source = 'google' and medium = 'cpc' then 'Google Ads'
        when source = '(direct)' and medium = '(none)' then 'Direct'
        when medium = 'organic' then 'Organic Search'
        when medium = 'referral' then 'Referral'
        when source = 'IGShopping' and medium = 'Social' then 'Facebook Organic'
        when lower(source) IN ('facebook','fb','ig','facebook_ads')  then 'Facebook Ads'
        when source = 'socialmedia' then 'Social Media'
        when source like '%mail%' then 'E-mail'
        else 'Não Identificado'
    end as canal,
    case 
        when source = 'google' and medium = 'cpc' and campaign!= '(direct)' then campaign
        when source = 'google' and medium = 'cpc' and campaign= '(direct)' then 'NA'
        when source = '(direct)' and medium = '(none)' then '(direct)'
        when medium = 'organic' then CONCAT(source,' ',medium)
        when medium = 'referral' then source
        when source = 'IGShopping' and medium = 'Social' then 'Facebook Organic'
        when lower(source) IN ('facebook','fb','ig','facebook_ads')  then campaign
        when source = 'socialmedia' then campaign
        when source like '%mail%' then campaign
        else 'Não Identificado'
    end as trafego,
    ec.* except(content),
    coalesce(g.content,ec.content) as content
  from 
    cte_enhanced_campaign ec 
  left join 
    cte_gclid_mapping g 
  on 
    ec.gclid = g.gclid
)
, cte_manual_fixes as (
    select 
    CASE 
        WHEN trafego = '26-12-2023 AD Vendas Campanha'
        THEN '[AD][ADV][Vendas][26-12-2023]'
        when trafego = '26-12-2023+AD+Vendas+Campanha'
        THEN '[AD][ADV][Vendas][26-12-2023]'
        ELSE trafego
    END trafego,
    CASE 
        WHEN medium = 'cpc_22-09_interesses_mullheres_21-45_'
        THEN 'cpc_22-09_interesses_mullheres_21-45'
        when medium = '26-12-2023 AD Vendas Conjunto de anúncios'
        then '[Planners]'
        when medium = '26-12-2023+AD+Vendas+Conjunto+de+anúncios'
        then '[Planners]'
        ELSE medium
    END medium,
    CASE 
        WHEN campaign = '26-12-2023 AD Vendas Campanha'
        THEN '[AD][ADV][Vendas][26-12-2023]'
        when campaign = '26-12-2023+AD+Vendas+Campanha'
        THEN '[AD][ADV][Vendas][26-12-2023]'
        ELSE campaign
    END campaign,
    CASE 
        WHEN medium = 'cpc_22-09_interesses_mullheres_21-45_'
            AND campaign = 'abo_conversao29-06'
            AND content is null
        THEN 'reels_planner-perf'

        WHEN medium = 'cpc_envolvidos30d'
            AND campaign = 'abo_rmkt'
            AND content is null
        THEN 'catalogo_todos_os_produtos'

        WHEN medium = 'cpc_novos_interesses_sud_mulheres24-45'
            AND campaign = 'abo_conversao29-06'
            AND content is null
        THEN 'vitrine_img_unica_fretegratis'

        WHEN medium = 'cpc_novos_interesses_sud_mulheres24-45'
            AND campaign = 'abo_conversao29-06'
            AND content = 'img_unica_fretegratis'
        THEN 'vitrine_img_unica_fretegratis'
        when content = '01 | Prova Social'
        then '01|Prova Social'
        ELSE content
    END content,
    * except(trafego, medium, content, campaign)
    from cte_traffic_attribution
)
, cte_facebook_ads_mapping as (
  select 
    distinct
    ee.session_key ,
    fads.campaign_name as campaign,
    fads.adset_name as content,
  from 
    {{ source('facebook_ads_graph_api','p_ads_AdsBasicStats') }} fads
  left join
    {{ source('sheets','mapping_facebook_ads') }} m
  on 
    cast(fads.ad_id as string) = cast(m.ad_id as string)
  left join
    cte_manual_fixes ee 
  on 
    ee.source = m.source
    AND ee.medium = m.medium
    AND ee.campaign = m.campaign
    AND ee.content = m.content
    AND date(ee.base_date) = date(fads.date_start)
  WHERE
    1=1
    AND ee.canal = 'Facebook Ads'
)
, cte_internal_traffic as (
  select distinct
    session_key,
    'Direct' as canal,
    '(direct)' as trafego,
    '(direct)' as source,
    '(none)' as medium,
    cast(null as string) as campaign,
  from 
    cte_traffic_attribution
  where
    1=1
    and event_name = 'session_start'
    and page_referrer like '%auciello%'
    and (
      medium in ('organic') -- google organic
    )
)
, cte_fact_session as (
  select distinct
    coalesce(it.canal, m.canal) as canal,
    coalesce(f.campaign, it.trafego, m.trafego) as trafego,
    coalesce(it.source, m.source) as source,
    coalesce(it.medium, m.medium) as medium,
    coalesce(f.campaign, it.campaign, m.campaign) as campaign,
    coalesce(f.content, m.content) as content,
    max(m.term)  over(partition by m.session_key) as term,
    m.* except(canal, trafego, source, medium, campaign, content, term, user_properties, ecommerce, fbclid),
    max( 
        coalesce(
            fbclid,
            REGEXP_EXTRACT(page_location, r'[?&]fbclid=([^&]*)'),
            REGEXP_EXTRACT(page_referrer, r'[?&]fbclid=([^&]*)')
        )
    ) over(partition by m.session_key) as fbclid,
    ecommerce.transaction_id as ecommerce_transaction_id,
    ecommerce.unique_items as ecommerce_unique_items,
    ecommerce.purchase_revenue as ecommerce_purchase_revenue,
  from 
    cte_manual_fixes m 
  left join 
    cte_facebook_ads_mapping f 
  on 
    m.session_key = f.session_key
  left join
    cte_internal_traffic it
  on 
  m.session_key = it.session_key
)

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
from
  cte_fact_session
where
  1=1
  and page_location not like '%https://gtm%'