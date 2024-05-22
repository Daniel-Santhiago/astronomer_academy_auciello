{{ 
  config(
    severity = 'error'
  ) 
}}
select
    date_diff(current_date(), max(metric_date), day) as delay_days
from
  {{ ref('fact_facebook_insights_device') }}
having
  date_diff(current_date(), max(metric_date), day) > 1