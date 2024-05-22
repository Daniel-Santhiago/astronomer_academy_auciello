{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_session_insights_daily_traffic_unpivot'
    )
}}


{% set lookback_window = 7 %} 
{% set days_range = range(0, lookback_window) %}


with 
RawData as (

  select 
      channel,
      traffic_name,
      source,
      medium,
      {% for day in days_range %}
      session_start_d_{{day}},
      add_to_cart_d_{{day}},
      begin_checkout_d_{{day}},
      purchase_d_{{day}},
      add_to_cart_rate_d_{{day}},
      -- begin_checkout_rate_d_{{day}},
      -- purchase_rate_d_{{day}},
      -- conversion_rate_d_{{day}},
      {% endfor %}
  from {{ ref('az_session_insights_daily_traffic')}}
  
),
UnpivotedData as (
  select day,
        channel,
        traffic_name,
        source,
        medium,
         session_start,
         add_to_cart,
         begin_checkout,
         purchase,
         add_to_cart_rate,
        --  begin_checkout_rate,
        --  purchase_rate,
        --  conversion_rate
  from RawData,
       UNNEST([
            {% for day in days_range %}
            STRUCT({{day}}                          as day, 
                   session_start_d_{{day}}          as session_start,
                   add_to_cart_d_{{day}}            as add_to_cart,
                   begin_checkout_d_{{day}}         as begin_checkout,
                   purchase_d_{{day}}               as purchase,
                   add_to_cart_rate_d_{{day}}       as add_to_cart_rate
                  --  begin_checkout_rate_d_{{day}}    as begin_checkout_rate,
                  --  purchase_rate_d_{{day}}          as purchase_rate,
                  --  conversion_rate_d_{{day}}        as conversion_rate

                   )
                   {% if not loop.last %}, {% endif %}  
            {% endfor %}
       ]) AS unpivot
)
select 
  date_add(date_trunc(current_date('America/Sao_Paulo'),day) , interval -day day) as day,
  channel,
  traffic_name,
  source,
  medium,
  session_start,
  add_to_cart,
  begin_checkout,
  purchase,
  add_to_cart_rate,
  -- begin_checkout_rate,
  -- purchase_rate,
  -- conversion_rate
from 
  UnpivotedData
order by
  day desc


