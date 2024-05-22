{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_linktree_mapping',
        enabled = False
    )
}}

with
cte_linktree_click as (
  select
    session_id,
    session_datetime,
    user_pseudo_id,
    fbclid
  from
    {{ ref('fact_session_linktree') }}
  where
    1=1
    and event_name = 'click'
    and session_datetime >= '2024-03-01'
  )
, cte_website_referral as (
select
  session_id,
  session_datetime,
  user_pseudo_id,
  fbclid
  -- *
from
  {{ ref('fact_session_website') }}
where
  1=1
  -- and event_name = 'purchase'
  -- and date(session_datetime) = '2024-03-29'
  -- and user_pseudo_id = '1324320572.1711410822'
  and event_name = 'session_start'
  and page_referrer = 'https://linktr.ee/'
  and session_datetime >= '2024-03-01'
)
select
  count(cte_linktree_click.session_id),
  count(distinct cte_linktree_click.session_id),
  count(cte_website_referral.session_id),
  count(distinct cte_website_referral.session_id),
  -- *
from
  cte_linktree_click
left join
  cte_website_referral
on
  cte_linktree_click.session_datetime between
    datetime_add(cte_website_referral.session_datetime, interval - 1 minute)
    and
    datetime_add(cte_website_referral.session_datetime, interval + 1 minute)
where
  1=1
  -- and fbclid is not null