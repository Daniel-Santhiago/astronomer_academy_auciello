{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_ads_insights_monthly_account_unpivot'
    )
}}


{% set lookback_window = 6 %} 
{% set months_range = range(0, lookback_window) %}


with 
RawData as (

  select 
    account_name,
      {% for month in months_range %}
      cost_m_{{month}},
      impressions_m_{{month}},
      video_view_m_{{month}},
      video_watch_25_m_{{month}},
      video_watch_50_m_{{month}},
      video_watch_75_m_{{month}},
      video_watch_100_m_{{month}},
      clicks_m_{{month}},
      cpm_m_{{month}},
      ctr_m_{{month}},
      add_to_cart_m_{{month}},
      initiate_checkout_m_{{month}},
      purchase_m_{{month}},
      purchase_value_m_{{month}},
      add_to_cart_rate_m_{{month}},
      initiate_checkout_rate_m_{{month}},
      purchase_rate_m_{{month}},
      roas_m_{{month}},
      -- conversion_rate_m_{{month}},
      {% endfor %}
  from {{ ref('az_facebook_ads_insights_monthly_account')}}
  
),
UnpivotedData as (
  select account_name,
         month,
         cost,
         impressions,
         video_view,
         video_watch_25,
         video_watch_50,
         video_watch_75,
         video_watch_100,
         clicks,
         cpm,
         ctr,
         add_to_cart,
         initiate_checkout,
         purchase,
         purchase_value,
         add_to_cart_rate,
         initiate_checkout_rate,
         purchase_rate,
         roas,
        --  conversion_rate
  from RawData,
       UNNEST([
            {% for month in months_range %}
            STRUCT({{month}}                                    as month, 
                   round(cost_m_{{month}},2)                    as cost,
                   round(impressions_m_{{month}},0)             as impressions,

                   round(video_view_m_{{month}},0)              as video_view,
                   round(video_watch_25_m_{{month}},0)          as video_watch_25,
                   round(video_watch_50_m_{{month}},0)          as video_watch_50,
                   round(video_watch_75_m_{{month}},0)          as video_watch_75,
                   round(video_watch_100_m_{{month}},0)         as video_watch_100,

                   round(clicks_m_{{month}},0)                  as clicks,
                   round(cpm_m_{{month}},4)                     as cpm,
                   round(ctr_m_{{month}},4)                     as ctr,
                   round(add_to_cart_m_{{month}},0)             as add_to_cart,
                   round(initiate_checkout_m_{{month}},0)       as initiate_checkout,
                   round(purchase_m_{{month}},0)                as purchase,
                   round(purchase_value_m_{{month}},2)          as purchase_value,
                   round(add_to_cart_rate_m_{{month}},4)        as add_to_cart_rate,
                   round(initiate_checkout_rate_m_{{month}},4)  as initiate_checkout_rate,
                   round(purchase_rate_m_{{month}},4)           as purchase_rate,
                  --  conversion_rate_m_{{month}}        as conversion_rate
                  round(roas_m_{{month}},2)                     as roas

                   )
                   {% if not loop.last %}, {% endif %}  
            {% endfor %}
       ]) AS unpivot
)
select 
  account_name,
  date_add(date_trunc(current_date('America/Sao_Paulo'),month) , interval -month month) as month,
  cost,
  impressions,
  video_view,
  video_watch_25,
  video_watch_50,
  video_watch_75,
  video_watch_100,
  clicks,
  add_to_cart,
  initiate_checkout,
  purchase,
  purchase_value,
  cpm,
  ctr,
  add_to_cart_rate,
  initiate_checkout_rate,
  purchase_rate,
  roas
  
from 
  UnpivotedData
order by
  month desc


