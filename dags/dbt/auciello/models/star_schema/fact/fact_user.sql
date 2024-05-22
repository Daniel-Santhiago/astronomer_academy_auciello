{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_user'
    )
}}



with
cte_user_with_client as (
  select 
    client_bling_id,
    client_name,
    client_mail,
    client_state,
    client_city,
    client_neighborhood,
    client_address,
    client_address_number,
    client_address_complement,
    user_id,
    user_pseudo_id,
    geo_country,
    geo_region,
    geo_city,
    platform,
    device_category,
    first_value(first_session_datetime) over(partition by client_bling_id order by first_session_datetime asc) as first_session_datetime,
    first_value(last_session_datetime)  over(partition by client_bling_id order by last_session_datetime desc) as last_session_datetime,
    
  from
    {{ ref('dim_user') }}
  where
    1=1
    and client_bling_id is not null
)
select
  * except(user_id, user_pseudo_id),
  split(string_agg(user_id)) as user_id,
  split(string_agg(user_pseudo_id)) as user_pseudo_id,
from
  cte_user_with_client
group by 
  all

union all

select 
  client_bling_id,
  client_name,
  client_mail,
  client_state,
  client_city,
  client_neighborhood,
  client_address,
  client_address_number,
  client_address_complement,
  geo_country,
  geo_region,
  geo_city,
  platform,
  device_category,
  first_session_datetime,
  last_session_datetime,
  split(user_id) as user_id,
  split(user_pseudo_id),
from
  {{ ref('dim_user') }}
where
  1=1
  and client_bling_id is null


