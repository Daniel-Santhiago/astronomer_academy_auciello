{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_creative_insights_all_time'
    )
}}


select
  account_name,
  campaign_name,
  adset_name,
  ad_name,
  -- metric_date, 
  min(metric_date)                                              as ad_start_date,
  max(metric_date)                                              as ad_end_date,
  count(distinct metric_date)                                   as days,
  --- qualidade
  round(safe_divide(sum(cost) , sum(impressions)) * 1000,2)     as cpm,
  round(safe_divide(sum(impressions) , sum(reach)),2)           as frequency,
  --- atenção
  round(safe_divide(sum(video_view) , sum(impressions)),2)      as video_view_3s_rate,
  round(safe_divide(sum(video_watch_25) , sum(video_view)),2)   as video_view_25_rate,
  --retencao
  round(safe_divide(sum(video_watch_50) , sum(video_view)),2)   as video_view_50_rate,
  round(safe_divide(sum(video_watch_75) , sum(video_view)),2)   as video_view_75_rate,
  round(safe_divide(sum(video_watch_95) , sum(video_view)),2)   as video_view_95_rate,
  --- acao
  round(safe_divide(sum(clicks) , sum(impressions)),4)          as ctr,
  round(safe_divide(sum(cost) , sum(clicks)),2)                 as cpc,
  --- vendas
  round(sum(purchase),0)                                        as purchase,
  round(safe_divide(sum(cost) , sum(purchase)),2)               as cpa,
  round(safe_divide(sum(purchase_value) , sum(cost)),2)         as roas,
  round(sum(cost),2)                                            as cost,
  -- round(sum(view_content),2)                                    as view_content,
  -- round(sum(landing_page_view),2)                               as landing_page_view,

  
  -- round(sum(impressions),0)                                     as impressions,
  -- round(sum(reach),0)                                           as reach,
  -- round(sum(video_view),0)                                      as video_view,
  -- round(sum(video_watch_25),0)                                  as video_watch_25,
  -- round(sum(video_watch_50),0)                                  as video_watch_50,
  -- round(sum(video_watch_75),0)                                  as video_watch_75,
  -- round(sum(video_watch_95),0)                                  as video_watch_95,
  -- round(sum(video_watch_100),0)                                 as video_watch_100,
  -- round(sum(clicks),2)                                          as clicks,

  -- *
from
  {{ ref('fact_facebook_insights_device') }}
where
  1=1
  -- and metric_date = '2024-04-19'
  and metric_date > '2023-12-26'
group by
  all
