{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_google_forms_isbn'
    )
}}

select distinct
  * except(amazon_url),
  amazon_url
from
  {{ source('isbndb','search_books_append') }}
qualify
  ROW_NUMBER() over (
    partition by form_id
    order by
      (case when original_query like '%978%' then 1 else 2 end) asc,
      (case when image_formula like '%4;180;150)%' then 1 else 2 end) asc
  ) = 1
order by
  form_datetime asc, tray_order_id asc, form_book_index asc

/*
---- Query para verificar se a tabela final possui registros duplicados pós ajuste manual

select 
  -- count(form_book_index) over(partition by form_datetime, tray_order_id, form_book_index),
  case when original_query not like '%978%' then 'DELETE' ELSE 'MANUAL INSERT'END as action,
  form_id,
  * 
from 
  `auciello-design.isbndb.search_books_append`
qualify
  count(form_book_index) over(partition by form_datetime, tray_order_id, form_book_index) > 1



------------------
--- -POST HOOK 
delete from `auciello-design.isbndb.search_books_append`
where form_id IN (
  select form_id 
  from `auciello-design.isbndb.search_books_append`
  qualify
  count(form_book_index) over(partition by form_datetime, tray_order_id, form_book_index) > 1
  and original_query not like '%978%'
  )


*/