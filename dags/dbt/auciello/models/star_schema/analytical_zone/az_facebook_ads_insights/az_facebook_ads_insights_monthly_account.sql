{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_ads_insights_monthly_account'
    )
}}


{% set lookback_window = 6 %}
{% set months_range = range(0, lookback_window) %}

select 
  datetime(TIMESTAMP_MILLIS(last_modified_time),'America/Sao_Paulo') AS updated_at,
  account_name,  

  -- Cost
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(cost as float64) end)              as cost_m_{{ month }},
  {% endfor %}

  -- Impressions
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(impressions as int64) end)         as impressions_m_{{ month }},
  -- Video Watch
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(video_view as int64) end)         as video_view_m_{{ month }},
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(video_watch_25 as int64) end)     as video_watch_25_m_{{ month }},
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(video_watch_50 as int64) end)     as video_watch_50_m_{{ month }},
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(video_watch_75 as int64) end)     as video_watch_75_m_{{ month }},
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(video_watch_100 as int64) end)     as video_watch_100_m_{{ month }},
  
  
  {% endfor %}

  -- Clicks
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(clicks as int64) end) as clicks_m_{{ month }},
  {% endfor %}

  -- Add to Cart
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(add_to_cart as int64) end) as add_to_cart_m_{{ month }},
  {% endfor %}

  -- Initiate Checkout
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(initiate_checkout as int64) end) as initiate_checkout_m_{{ month }},
  {% endfor %}

  -- Purchase
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(purchase as int64) end) as purchase_m_{{ month }},
  {% endfor %}

  -- Purchase Value
  {% for month in months_range %}
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(purchase_value as float64) end) as purchase_value_m_{{ month }},
  {% endfor %}

  -- CPM
  {% for month in months_range %}
  round(safe_divide(
  (sum( case 
          when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
          then cast(cost as float64) end) * 1000)
  ,
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(impressions as int64) end) 
  ),2)                                      as cpm_m_{{ month }},
  {% endfor %}

  -- CTR
  {% for month in months_range %}
  round(safe_divide(
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(clicks as int64) end)   
  ,
    sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(impressions as int64)  end)        
  ),4)                                 as ctr_m_{{ month }},
  {% endfor %}

  -- Add to Cart Rate
  {% for month in months_range %}
  round(safe_divide(
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(add_to_cart as int64) end)   
  ,
    sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(clicks as int64)  end)        
  ),4)                                 as add_to_cart_rate_m_{{ month }},
  {% endfor %}

  -- Initiate Checkout Rate
  {% for month in months_range %}
  round(safe_divide(
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(initiate_checkout as int64) end)   
  ,
    sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(add_to_cart as int64)  end)        
  ),4)                                 as initiate_checkout_rate_m_{{ month }},
  {% endfor %}

  -- Purchase Rate
  {% for month in months_range %}
  round(safe_divide(
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(purchase as int64) end)   
  ,
    sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(initiate_checkout as int64)  end)        
  ),4)                                 as purchase_rate_m_{{ month }},
  {% endfor %}

  -- ROAS
  {% for month in months_range %}
  round(safe_divide(
  sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(purchase_value as float64) end)   
  ,
    sum( case 
        when date_trunc(metric_date,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        then cast(cost as float64)  end)        
  ),4)                                 as roas_m_{{ month }},
  {% endfor %}



from 
  {{ ref('fact_facebook_insights_device') }}
cross join
  `auciello-design.star_schema.__TABLES__`
where
  1=1
  and date_trunc(metric_date,month) >= date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ lookback_window }} month)
  and table_id = 'fact_facebook_insights_device'
group by
  all