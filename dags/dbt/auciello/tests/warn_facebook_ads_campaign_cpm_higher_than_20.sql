{{ 
  config(
    severity = 'warn'
  ) 
}}
select 
  * 
from
  {{ ref('az_facebook_ads_insights_daily_campaign_unpivot') }}
where
  1=1
  and cpm > 20
  and day >= current_date('America/Sao_Paulo') - 1