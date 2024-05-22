{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_session_insights_daily_traffic'
    )
}}


{% set lookback_window = 7 %} 
{% set days_range = range(0, lookback_window) %}

select
  datetime(TIMESTAMP_MILLIS(last_modified_time),'America/Sao_Paulo') AS updated_at,
  channel,
  traffic_name,
  source,
  medium,
  {% for day in days_range %}
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'session_start' 
        then session_id end)              as session_start_d_{{ day }},
  {% endfor %}
  {% for day in days_range %}
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'add_to_cart' 
        then session_id end)              as add_to_cart_d_{{ day }},
  {% endfor %}
  {% for day in days_range %}
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'begin_checkout' 
        then session_id end)              as begin_checkout_d_{{ day }},
  {% endfor %}
  {% for day in days_range %}
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'purchase' 
        then session_id end)              as purchase_d_{{ day }},
  {% endfor %}

  {% for day in days_range %}
  round(
    safe_divide(
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'add_to_cart' 
        then session_id end)              
        ,
  count(distinct case when date(session_datetime) = current_date('America/Sao_Paulo') - {{ day }} 
        and event_name = 'session_start' 
        then session_id end)      
    ),4)
        as add_to_cart_rate_d_{{ day }},
  {% endfor %}

from
  {{ ref('fact_session_website') }}
cross join
  `auciello-design.intermediate.__TABLES__`
where
  1=1
  and session_datetime >= current_date('America/Sao_Paulo') - {{ lookback_window }}
  and table_id = 'ga4_enhanced_events'
group by
  all
