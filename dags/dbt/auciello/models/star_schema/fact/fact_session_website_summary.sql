{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_session_website_summary',
        partition_by={
          "field": "session_start_datetime",
          "data_type": "datetime",
          "granularity": "day"
        },
    )
}}

select distinct
  session_id,
  avg(ga_session_id) over(partition by session_id)                                          as ga_session_id,
  count(distinct event_id) over(partition by session_id)                                    as event_count,
  min(session_datetime) over(partition by session_id)                                       as session_start_datetime,
  max(session_datetime) over(partition by session_id)                                       as session_end_datetime,
  avg(session_number) over(partition by session_id)                                         as session_number,
  avg(session_engagement_time_sec) over(partition by session_id)                            as session_engagement_time_sec,
  first_value(geo_country) over(partition by session_id order by session_datetime desc)     as geo_country,
  first_value(geo_region) over(partition by session_id order by session_datetime desc)      as geo_region,
  first_value(geo_city) over(partition by session_id order by session_datetime desc)        as geo_city,

  first_value(page_location) over(partition by session_id order by session_datetime asc)    as entry_page,
  first_value(page_location) over(partition by session_id order by session_datetime desc)   as exit_page,

  first_value(page_path) over(partition by session_id order by session_datetime asc)        as entry_page_path,
  first_value(page_path) over(partition by session_id order by session_datetime desc)       as exit_page_path,

  first_value(channel) over(partition by session_id order by session_datetime desc)         as channel,
  first_value(traffic_name) over(partition by session_id order by session_datetime desc)    as traffic_name,
  first_value(source) over(partition by session_id order by session_datetime desc)          as source,
  first_value(medium) over(partition by session_id order by session_datetime desc)          as medium,
  first_value(campaign_name) over(partition by session_id order by session_datetime desc)   as campaign_name,
  first_value(content) over(partition by session_id order by session_datetime desc)         as content,
  first_value(term) over(partition by session_id order by session_datetime desc)            as term,
  first_value(gclid) over(partition by session_id order by session_datetime desc)           as gclid,
  first_value(fbclid) over(partition by session_id order by session_datetime desc)          as fbclid,

  first_value(user_id) over(partition by session_id order by session_datetime desc)         as user_id,
  first_value(user_pseudo_id) over(partition by session_id order by session_datetime desc)  as user_pseudo_id,

  first_value(device_category) over(partition by session_id order by session_datetime desc) as device_category,

from
  {{ ref('fact_session_website') }}
where
  1=1
  -- and date(session_datetime) = '2024-04-30'
  -- and session_id = 'bdc12GxsB8UUSyCnF9kVvQ=='
