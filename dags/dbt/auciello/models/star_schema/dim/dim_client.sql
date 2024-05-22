{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_client'
    )
}}

select
  bp.cliente_id               as client_bling_id,
  bp.cliente_nome             as client_name,

  bp.cliente_cnpj             as client_fiscal_id,
  bp.cliente_fone             as client_phone,
  bp.cliente_email            as client_mail,

  bp.cliente_uf               as client_state,
  bp.cliente_cidade           as client_city,
  bp.cliente_bairro           as client_neighborhood,
  bp.cliente_endereco         as client_address,
  bp.cliente_numero           as client_address_number,
  bp.cliente_complemento      as client_address_complement,

from
  {{ source('kondado','bling_pedidos') }} bp
qualify
  row_number() over(partition by cliente_id order by data desc) = 1