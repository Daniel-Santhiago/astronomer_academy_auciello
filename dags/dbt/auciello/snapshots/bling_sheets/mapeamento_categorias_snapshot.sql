{% snapshot bling_sheets_mapeamento_categorias_snapshot %}

{{
    config(
      target_database='auciello-design',
      target_schema='snapshots',
      unique_key="id_categoria",
      alias = 'mapeamento_categorias',
      strategy='check',
      check_cols=[
        'descricao',
        'lucro_desejado',
        'perc_reserva',
        'perc_caixa',
        'perc_salario'
      ],
      enabled=true
    )
}}

select distinct 
    * 
from {{ source('bling_sheets','mapeamento_categorias') }}



{% endsnapshot %}