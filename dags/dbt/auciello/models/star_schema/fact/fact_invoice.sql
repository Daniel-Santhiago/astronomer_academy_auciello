{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_invoice'
    )
}}

select
  nota_numero                 as invoice_id,
  nota_dataemissao            as invoice_date,
  nota_valornota              as invoice_value,
  nota_chaveacesso            as invoice_access_key,
  nota_serie                  as invoice_series,
  bp.cliente_id               as client_bling_id,
  bp.numero                   as bling_order_id,
  bp.numeropedidoloja         as tray_order_id,
from
  {{ source('kondado','bling_pedidos') }} bp
where
  1=1
  and nota_numero is not null