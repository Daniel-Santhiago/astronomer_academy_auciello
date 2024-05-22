{% snapshot bling_sheets_produtos_snapshot %}

{{
    config(
      target_database='auciello-design',
      target_schema='snapshots',
      unique_key="id||'-'||coalesce(cast(estrutura_componentes_produto_id as string),'')",
      alias = 'produtos',
      strategy='check',
      check_cols=[
        'nome','codigo','tipo','formato','preco','categoria_id', 'observacoes',
        'estrutura_componentes_produto_id','estrutura_componentes_quantidade','estrutura_componentes_produto_nome',
        'estrutura_componentes_produto_codigo','estrutura_componentes_produto_preco'
      ],
      enabled=true
    )
}}

select distinct 
    id||'-'||coalesce(cast(estrutura_componentes_produto_id as string),'') as k,
    * 
from {{ source('bling_sheets','produtos') }}



{% endsnapshot %}