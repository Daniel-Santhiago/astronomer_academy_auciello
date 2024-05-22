{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_session_website',
        partition_by={
          "field": "session_datetime",
          "data_type": "datetime",
          "granularity": "day"
        },
    )
}}

select
  *
from
  {{ ref('fact_session') }}
where
  1=1
  and page_location like '%https://www.auciellodesign.com%'