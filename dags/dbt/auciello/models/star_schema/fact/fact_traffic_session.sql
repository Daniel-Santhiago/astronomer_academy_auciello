
{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_traffic_session',
        sql_header='''
        CREATE TEMPORARY FUNCTION DECODE_URI_COMPONENT(path STRING)
        RETURNS STRING
        LANGUAGE js AS """
          if (path == null) return null;
          try {
            return decodeURIComponent(path);
          } catch (e) {
            return path;
          }
        """;
        '''
        
    )
}}

with
cte_fetch_and_decode_utm as (
  select
    session_key,
    source_event_level,
    source_user_level,
    medium_event_level,
    medium_user_level,
    campaign_event_level,
    campaign_user_level,
    content,
    term,
    gclid,
    fbclid,

    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_source=([^&#]+)'))     AS utm_source,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_medium=([^&#]+)'))     AS utm_medium,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_campaign=([^&#]+)') )  AS utm_campaign,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_content=([^&#]+)'))    AS utm_content,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_term=([^&#]+)'))       AS utm_term,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'utm_id=([^&#]+)') )        AS utm_id,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'gclid=([^&#]+)') )         AS utm_gclid,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_location, r'fbclid=([^&#]+)'))         AS utm_fbclid,

    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_source=([^&#]+)') )    AS utm_source_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_medium=([^&#]+)'))     AS utm_medium_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_campaign=([^&#]+)'))   AS utm_campaign_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_content=([^&#]+)') )   AS utm_content_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_term=([^&#]+)'))       AS utm_term_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'utm_id=([^&#]+)') )        AS utm_id_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'gclid=([^&#]+)') )         AS utm_gclid_referrer,
    DECODE_URI_COMPONENT(REGEXP_EXTRACT(page_referrer, r'fbclid=([^&#]+)') )        AS utm_fbclid_referrer,


  from
    {{ ref('stg_incremental_fact_session') }}
  where
    1=1
    -- and base_date = '2024-05-11'
    -- and user_pseudo_id = '1776919432.1715430533'
)
, cte_traffic_mapping as (
  select
    session_key,
    replace(coalesce(source_event_level, utm_source, utm_source_referrer, source_user_level,'(direct)'),'+',' ')       as source,
    replace(coalesce(medium_event_level, utm_medium, utm_medium_referrer, medium_user_level, '(none)'),'+',' ')         as medium,
    replace(coalesce(campaign_event_level, utm_campaign, utm_campaign_referrer, campaign_user_level),'+',' ')            as campaign,
    replace(coalesce(content, utm_content, utm_content_referrer),'+',' ')                           as content,
    replace(coalesce(term, utm_term, utm_term_referrer),'+',' ')                                    as term,
    replace(coalesce(gclid, utm_gclid, utm_gclid_referrer),'+',' ')                                 as gclid,
    replace(coalesce(fbclid, utm_fbclid, utm_fbclid_referrer),'+',' ')                              as fbclid,
  from
    cte_fetch_and_decode_utm
)
select 
  *
from
  cte_traffic_mapping
qualify
  row_number() over(
          partition by session_key
          order by
          (case when source = '(direct)' then 1 else 2 end) desc
          ) = 1