{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_order'
    )
}}


SELECT 
  bp.numero                   as bling_order_id,
  bp.numeropedidoloja         as tray_order_id,
  nota_numero                 as invoice_id,
  bp.cliente_id               as client_bling_id,
  bp.situacao                 as order_status,
  datasaida                   as order_date,
  totalvenda                  as order_sales_value,
  totalprodutos               as order_products_value,
  desconto                    as order_discount_value,
  itens_descontoitem          as order_discount_item_value,
  valorfrete                  as shipping_value,
  coalesce(
    v.preco_conferido/100, 0.0
  )                           as shipping_paid_value,
  i.itens_codigo              as product_code,
  i.itens_quantidade          as product_quantity,
  observacoes                 as order_comments,
  regexp_extract(
      observacoes, 
      r'Pedido em (\d+) vez'
      )                       as installments_quantity,
  case 
    when observacoes like '%Pix - Vindi%' and totalvenda * (0.95/100) <= 1.6 then 1.6
    when observacoes like '%Pix - Vindi%' and totalvenda * (0.95/100) > 1.6 then round((totalvenda * (0.95/100)),2)
    when observacoes like '%Boleto%' then 2.99
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '1' then round((4.49)/100 * totalvenda ,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '2' then round((4.49 + 2.99)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '3' then round((4.49 + 4.01)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '4' then round((4.49 + 5.02)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '5' then round((4.49 + 6.05)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '6' then round((4.49 + 7.08)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '7' then round((4.49 + 8.12)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '8' then round((4.49 + 9.16)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '9' then round((4.49 + 10.2)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '10' then round((4.49 + 11.2)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '11' then round((4.49 + 12.3)/100 * totalvenda,2)
    when observacoes like '%Cartão%' and REGEXP_EXTRACT(observacoes, r'Pedido em (\d+) vez') = '12' then round((4.49 + 13.4)/100 * totalvenda,2)

  end                         as vindi_tax,

from
  -- `auciello-design`.`kondado`.`bling_pedidos` bp
  {{ source('kondado','bling_pedidos') }} bp
left join 
  -- `auciello-design`.`kondado`.`bling_pedidos_itens` i
  {{ source('kondado','bling_pedidos_itens') }} i
ON 
    i.numero_pedido = bp.numero
left join 
    {{ source('sheets','melhor_envio_frete') }} v
    -- sheets.melhor_envio_frete v
ON 
    bp.numeropedidoloja = REGEXP_EXTRACT(v.lembrete, r'Tray #(\d+)')
where
  1=1
  -- itens_descontoitem != 0
  -- and bp.situacao = 'Atendido'
