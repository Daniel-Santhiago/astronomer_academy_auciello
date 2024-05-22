{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_ads_insights_daily_campaign'
    )
}}


{% set lookback_window = 7 %} 
{% set days_range = range(0, lookback_window) %}

select 
  datetime(TIMESTAMP_MILLIS(last_modified_time),'America/Sao_Paulo') AS updated_at,
  account_name,  
  campaign_name,
  -- adset_name,
  -- ad_name,

-- Cost
  {% for day in days_range %}
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }} 
        then cast(cost as float64) end)             as cost_d_{{ day }},
  -- Impressions
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }} 
        then cast(impressions as int64) end)        as impressions_d_{{ day }},
  -- Video Watch
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }}
        then cast(video_view as int64) end)         as video_view_d_{{ day }},
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }}
        then cast(video_watch_25 as int64) end)     as video_watch_25_d_{{ day }},
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }}
        then cast(video_watch_50 as int64) end)     as video_watch_50_d_{{ day }},
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }}
        then cast(video_watch_75 as int64) end)     as video_watch_75_d_{{ day }},
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }}
        then cast(video_watch_100 as int64) end)    as video_watch_100_d_{{ day }},
  -- Clicks
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }} 
        then cast(clicks as int64) end)             as clicks_d_{{ day }},
  -- Add to Cart
  sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(add_to_cart as int64) end)        as add_to_cart_d_{{ day }},
  -- Initiate Checkout
  sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(initiate_checkout as int64) end)  as initiate_checkout_d_{{ day }},
  -- Purchase
  sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(purchase as int64) end)           as purchase_d_{{ day }},
  -- Purchase Value
  sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(purchase_value as float64) end) as purchase_value_d_{{ day }},
  -- CPM
  round(safe_divide(
  (sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }} 
        then cast(cost as float64) end) * 1000)
    ,
  sum( case 
        when date(metric_date) = current_date('America/Sao_Paulo') - {{ day }} 
        then cast(impressions as int64) end) 
  ),2)                                            as cpm_d_{{ day }},
  -- Video View Rate
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_watch_25 as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_view as int64)  end)        
  ),4)                                            as video_watch_25_rate_d_{{ day }},
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_watch_50 as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_view as int64)  end)        
  ),4)                                            as video_watch_50_rate_d_{{ day }},
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_watch_75 as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_view as int64)  end)        
  ),4)                                            as video_watch_75_rate_d_{{ day }},
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_watch_100 as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(video_view as int64)  end)        
  ),4)                                            as video_watch_100_rate_d_{{ day }},


  -- CTR
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(clicks as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(impressions as int64)  end)        
  ),4)                                            as ctr_d_{{ day }},
  -- Add to Cart Rate
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(add_to_cart as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(clicks as int64)  end)        
  ),4)                                            as add_to_cart_rate_d_{{ day }},
  -- Initiate Checkout Rate
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(initiate_checkout as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(add_to_cart as int64)  end)        
  ),4)                                            as initiate_checkout_rate_d_{{ day }},
  -- Purchase Rate
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(purchase as int64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(initiate_checkout as int64)  end)        
  ),4)                                            as purchase_rate_d_{{ day }},
  -- CPA
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(cost as float64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(purchase as float64)  end)        
  ),4)                                            as cpa_d_{{ day }},
  -- ROAS
  round(safe_divide(
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(purchase_value as float64) end)   
    ,
    sum( case 
        when date_trunc(metric_date,day) = date_add( date_trunc(current_date('America/Sao_Paulo'),day), interval -{{ day }} day)
        then cast(cost as float64)  end)        
  ),4)                                            as roas_d_{{ day }},
  {% endfor %}


from 
  {{ ref('fact_facebook_insights_device') }}
cross join
  `auciello-design.star_schema.__TABLES__`
where
  date(metric_date) >= current_date('America/Sao_Paulo') - {{ lookback_window }}
  and table_id = 'fact_facebook_insights_device'
group by
  all