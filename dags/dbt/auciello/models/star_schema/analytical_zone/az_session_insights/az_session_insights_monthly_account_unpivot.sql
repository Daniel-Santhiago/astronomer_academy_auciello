{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_session_insights_monthly_account_unpivot'
    )
}}


{% set lookback_window = 6 %} 
{% set months_range = range(0, lookback_window) %}


with 
RawData as (

  select 
      {% for month in months_range %}
      session_start_m_{{month}},
      add_to_cart_m_{{month}},
      begin_checkout_m_{{month}},
      purchase_m_{{month}},
      add_to_cart_rate_m_{{month}},
      begin_checkout_rate_m_{{month}},
      purchase_rate_m_{{month}},
      conversion_rate_m_{{month}},
      {% endfor %}
  from {{ ref('az_session_insights_monthly_account')}}
  
),
UnpivotedData as (
  select month,
         session_start,
         add_to_cart,
         begin_checkout,
         purchase,
         add_to_cart_rate,
         begin_checkout_rate,
         purchase_rate,
         conversion_rate
  from RawData,
       UNNEST([
            {% for month in months_range %}
            STRUCT({{month}}                          as month, 
                   session_start_m_{{month}}          as session_start,
                   add_to_cart_m_{{month}}            as add_to_cart,
                   begin_checkout_m_{{month}}         as begin_checkout,
                   purchase_m_{{month}}               as purchase,
                   add_to_cart_rate_m_{{month}}       as add_to_cart_rate,
                   begin_checkout_rate_m_{{month}}    as begin_checkout_rate,
                   purchase_rate_m_{{month}}          as purchase_rate,
                   conversion_rate_m_{{month}}        as conversion_rate

                   )
                   {% if not loop.last %}, {% endif %}  
            {% endfor %}
       ]) AS unpivot
)
select 
  date_add(date_trunc(current_date('America/Sao_Paulo'),month) , interval -month month) as month,
  session_start,
  add_to_cart,
  begin_checkout,
  purchase,
  add_to_cart_rate,
  begin_checkout_rate,
  purchase_rate,
  conversion_rate
from 
  UnpivotedData
order by
  month desc


