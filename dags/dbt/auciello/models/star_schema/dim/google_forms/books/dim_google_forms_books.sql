{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_google_forms_books'
    )
}}



with
cte_aux_dim_google_forms as (

  select * from {{ ref('dim_google_forms_books_9_titles') }}
  union all
  select * from {{ ref('dim_google_forms_books_18_titles') }}
  union all
  select * from {{ ref('dim_google_forms_books_27_titles') }}
  union all
  select * from {{ ref('dim_google_forms_books_54_titles') }}

)

, cte_book_split as (
  select
    *,
    book                                                          as original_query,
    replace(book,'\n',' ')                                        as search_query,
    split(book,'\n')[safe_offset(0)]                              as book_title_linebreak,
    split(book,'\n')[safe_offset(1)]                              as book_author_linebreak,
    split(book,'\n')[safe_offset(2)]                              as book_publisher_linebreak,
    split(split(book,'\n')[safe_offset(0)],',')[safe_offset(0)]   as book_title_comma,
    split(split(book,'\n')[safe_offset(0)],',')[safe_offset(1)]   as book_author_comma,
    split(split(book,'\n')[safe_offset(0)],',' )[safe_offset(2)]  as book_publisher_comma,
  from
    cte_aux_dim_google_forms
  where
    1=1
)
, cte_user_books as (
select
  az_purchase_sessions.tray_order_id,
  az_purchase_sessions.client_name,
  to_base64(md5(concat(form_datetime,'|',email,
                      '|',covers,'|',form_book_index,'|',search_query)))        as form_id,
  datetime(form_datetime)                                                       as form_datetime,
  email,
  covers,
  form_book_index,
  original_query,
  search_query,
  book,
  trim(coalesce(book_title_comma, book_title_linebreak))                        as user_book_title,
  trim(coalesce(book_author_comma, book_author_linebreak))                      as user_book_author,
  trim(coalesce(book_publisher_comma, book_publisher_linebreak))                as user_book_publisher,
from
  cte_book_split
left join
  {{ ref('az_purchase_sessions') }} az_purchase_sessions
on
  lower(cte_book_split.email) = lower(az_purchase_sessions.client_mail)
  and datetime(cte_book_split.form_datetime) > az_purchase_sessions.session_datetime
  and cte_book_split.covers = REGEXP_EXTRACT(tray_product_name, r'(\d+)')
where
  1=1
  and tray_product_name like '%Monte sua cartela%'
)
select  
  *,
  concat(
      "https://www.amazon.com.br/s?k=",
      coalesce(replace(user_book_title,' ','+'),'') ,
      '+',
      coalesce(replace(user_book_author,' ','+'),''),
      '+',
      coalesce(replace(user_book_publisher,' ','+'),''),
      "&i=stripbooks&s=relevanceexprank&Adv-Srch-Books-Submit.x=0&Adv-Srch-Books-Submit.y=0&unfiltered=1&ref=sr_adv_b"
    )                                                                 as amazon_url
from
  cte_user_books
order by
  form_datetime, tray_order_id, form_book_index

