{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_google_forms_books_9_titles'
    )
}}




{% set covers = 9 %} 
{% set books = range(1, covers + 1) %}

{% for book in books %}
SELECT 
  Carimbo_de_data_hora                                          as form_datetime,
  Endere__o_de_e_mail                                           as email,
  '9'                                                           as covers,
  {{ loop.index}}                                               as form_book_index,
  Livro_{{ '%02d'|format(book) }}____Nome___Escritor___Editora  as book
FROM 
  {{ source('sheets','google_forms_minhas_leituras_9titulos') }}

{% if not loop.last %} UNION ALL {% endif %}  
{% endfor %}


