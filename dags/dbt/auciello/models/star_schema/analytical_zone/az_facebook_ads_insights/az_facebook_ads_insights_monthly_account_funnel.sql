{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_facebook_ads_insights_monthly_account_funnel'
    )
}}

{% set metrics = ["impressions", "clicks", "add_to_cart", "initiate_checkout", "purchase"] %}

{%- for metric in metrics %}
  select
    account_name      as account_name,
    month             as month,
    '{{ metric }}'    as metric,
    {{ metric }}      as value 

  FROM 
    {{ ref('az_facebook_ads_insights_monthly_account_unpivot') }}
  {%- if not loop.last %}UNION ALL {% endif -%}
{%- endfor %}

