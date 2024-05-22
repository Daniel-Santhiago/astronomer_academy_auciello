{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_session_insights_monthly_traffic'
    )
}}


{% set lookback_window = 6 %} 
{% set months_range = range(0, lookback_window) %}

select
  datetime(TIMESTAMP_MILLIS(last_modified_time),'America/Sao_Paulo') AS updated_at,
  channel,
  traffic_name,
  source,
  medium,
  {% for month in months_range %}
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'session_start' 
        then session_id end)              as session_start_m_{{ month }},

  {% endfor %}
  {% for month in months_range %}
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'add_to_cart' 
        then session_id end)              as add_to_cart_m_{{ month }},

  {% endfor %}      

  {% for month in months_range %}
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'begin_checkout' 
        then session_id end)              as begin_checkout_m_{{ month }},

  {% endfor %}
  {% for month in months_range %}
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'purchase' 
        then session_id end)              as purchase_m_{{ month }},

  {% endfor %}
  

  {% for month in months_range %}
  round(
    safe_divide(
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'add_to_cart' 
        then session_id end)               
        ,
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'session_start' 
        then session_id end)     
    ),4)
        as add_to_cart_rate_m_{{ month }},
  {% endfor %}


  {% for month in months_range %}
  round(
    safe_divide(
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'begin_checkout' 
        then session_id end)               
        ,
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'add_to_cart' 
        then session_id end)     
    ),4)
        as begin_checkout_rate_m_{{ month }},
  {% endfor %}



  {% for month in months_range %}
  round(
    safe_divide(
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'purchase' 
        then session_id end)               
        ,
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'begin_checkout' 
        then session_id end)     
    ),4)
        as purchase_rate_m_{{ month }},
  {% endfor %}

  {% for month in months_range %}
  round(
    safe_divide(
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'purchase' 
        then session_id end)               
        ,
  count(distinct case when date_trunc(session_datetime,month) = date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ month }} month)
        and event_name = 'session_start' 
        then session_id end)     
    ),4)
        as conversion_rate_m_{{ month }},
  {% endfor %}

from
  {{ ref('fact_session_website') }}
cross join
  `auciello-design.intermediate.__TABLES__`
where
  1=1
  and date_trunc(session_datetime,month) >= date_add( date_trunc(current_date('America/Sao_Paulo'),month), interval -{{ lookback_window }} month)
  and table_id = 'ga4_enhanced_events'
group by
  all
order by
  6 desc