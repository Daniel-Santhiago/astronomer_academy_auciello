{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_session_linktree',
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
  and page_location like '%https://linktr.ee%'
  -- and base_date >= '2024-01-01'
  -- and base_date = "2024-03-17"