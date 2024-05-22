{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_ads_insights_daily_campaign_unpivot'
    )
}}


{% set lookback_window = 7 %} 
{% set days_range = range(0, lookback_window) %}


with 
RawData as (

  select 
    account_name,
    campaign_name,
      {% for day in days_range %}
      cost_d_{{day}},
      impressions_d_{{day}},
      video_view_d_{{day}},
      video_watch_25_d_{{day}},
      video_watch_50_d_{{day}},
      video_watch_75_d_{{day}},
      video_watch_100_d_{{day}},
      clicks_d_{{day}},
      cpm_d_{{day}},
      video_watch_25_rate_d_{{day}},
      video_watch_50_rate_d_{{day}},
      video_watch_75_rate_d_{{day}},
      video_watch_100_rate_d_{{day}},
      ctr_d_{{day}},
      add_to_cart_d_{{day}},
      initiate_checkout_d_{{day}},
      purchase_d_{{day}},
      purchase_value_d_{{day}},
      add_to_cart_rate_d_{{day}},
      initiate_checkout_rate_d_{{day}},
      purchase_rate_d_{{day}},
      -- conversion_rate_d_{{day}},
      cpa_d_{{day}},
      roas_d_{{day}},
      {% endfor %}
  from {{ ref('az_facebook_ads_insights_daily_campaign')}}
  
),
UnpivotedData as (
  select account_name,
         campaign_name,
         day,
         cost,
         impressions,
         video_view,
         video_watch_25,
         video_watch_50,
         video_watch_75,
         video_watch_100,
         clicks,
         cpm,
         video_watch_25_rate,
         video_watch_50_rate,
         video_watch_75_rate,
         video_watch_100_rate,
         ctr,
         add_to_cart,
         initiate_checkout,
         purchase,
         purchase_value,
         add_to_cart_rate,
         initiate_checkout_rate,
         purchase_rate,
        --  conversion_rate,
         cpa,
         roas
  from RawData,
       UNNEST([
            {% for day in days_range %}
            STRUCT({{day}}                                    as day, 
                   round(cost_d_{{day}},2)                    as cost,
                   round(impressions_d_{{day}},0)             as impressions,
                   round(video_view_d_{{day}},0)              as video_view,
                   round(video_watch_25_d_{{day}},0)          as video_watch_25,
                   round(video_watch_50_d_{{day}},0)          as video_watch_50,
                   round(video_watch_75_d_{{day}},0)          as video_watch_75,
                   round(video_watch_100_d_{{day}},0)         as video_watch_100,
                   round(clicks_d_{{day}},0)                  as clicks,
                   round(cpm_d_{{day}},4)                     as cpm,
                   round(video_watch_25_rate_d_{{day}},4)     as video_watch_25_rate,
                   round(video_watch_50_rate_d_{{day}},4)     as video_watch_50_rate,
                   round(video_watch_75_rate_d_{{day}},4)     as video_watch_75_rate,
                   round(video_watch_100_rate_d_{{day}},4)    as video_watch_100_rate,
                   round(ctr_d_{{day}},4)                     as ctr,
                   round(add_to_cart_d_{{day}},0)             as add_to_cart,
                   round(initiate_checkout_d_{{day}},0)       as initiate_checkout,
                   round(purchase_d_{{day}},0)                as purchase,
                   round(purchase_value_d_{{day}},2)          as purchase_value,
                   round(add_to_cart_rate_d_{{day}},4)        as add_to_cart_rate,
                   round(initiate_checkout_rate_d_{{day}},4)  as initiate_checkout_rate,
                   round(purchase_rate_d_{{day}},4)           as purchase_rate,
                  --  round(conversion_rate_d_{{day}},4)         as conversion_rate,
                   round(cpa_d_{{day}} ,2)                    as cpa,
                   round(roas_d_{{day}} ,2)                   as roas

                   )
                   {% if not loop.last %}, {% endif %}  
            {% endfor %}
       ]) AS unpivot
)
select 
  account_name,
  campaign_name,
  date_add(date_trunc(current_date('America/Sao_Paulo'),day) , interval -day day) as day,
  cost,
  impressions,
  video_view,
  video_watch_25,
  video_watch_50,
  video_watch_75,
  video_watch_100,
  clicks,
  cpm,
  video_watch_25_rate,
  video_watch_50_rate,
  video_watch_75_rate,
  video_watch_100_rate,
  ctr,
  add_to_cart,
  initiate_checkout,
  purchase,
  purchase_value,
  add_to_cart_rate,
  initiate_checkout_rate,
  purchase_rate,
  -- conversion_rate,
  cpa,
  roas
from 
  UnpivotedData
order by
  day desc


