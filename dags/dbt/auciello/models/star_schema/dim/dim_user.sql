{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_user'
    )
}}

select distinct
  dim_client.client_bling_id,
  dim_client.client_name,
  fact_session.user_id,
  fact_session.user_pseudo_id,
  first_value(geo_country) over(partition by 
  case when client_bling_id is not null then client_bling_id else user_pseudo_id end order by fact_session.session_datetime desc)     as geo_country,
  first_value(geo_region) over(partition by 
  case when client_bling_id is not null then client_bling_id else user_pseudo_id end  order by fact_session.session_datetime desc)    as geo_region,
  first_value(geo_city) over(partition by 
  case when client_bling_id is not null then client_bling_id else user_pseudo_id end  order by fact_session.session_datetime desc)    as geo_city,
  first_value(platform) over(partition by 
  case when client_bling_id is not null then client_bling_id else user_pseudo_id end  order by fact_session.session_datetime desc)    as platform,
  first_value(device_category) over(partition by 
  case when client_bling_id is not null then client_bling_id else user_pseudo_id end  order by fact_session.session_datetime desc)    as device_category,
  dim_client.client_mail,
  dim_client.client_state,
  dim_client.client_city,
  dim_client.client_neighborhood,
  dim_client.client_address,
  dim_client.client_address_number,
  dim_client.client_address_complement,
  first_value(fact_session.session_datetime) over(partition by user_pseudo_id   order by fact_session.session_datetime asc)           as first_session_datetime,
  first_value(fact_session.session_datetime) over(partition by user_pseudo_id   order by fact_session.session_datetime desc)          as last_session_datetime,
from
  {{ ref('fact_session') }} 
left join 
  {{ ref('dim_product') }}
using(tray_product_id)
left join
  {{ ref('fact_order') }}
using(tray_order_id, product_code)
left join
  {{ ref('dim_client') }}  
using(client_bling_id)
where
  1=1

