{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_production_details'
    )
}}

select distinct
  fact_order.tray_order_id,
  fact_order.bling_order_id,
  dim_client.client_name,
  fact_order.order_status,
  fact_order.order_date,
  fact_order.product_code,
  fact_order.product_quantity,
  coalesce(
    dim_product_child_level_2.product_child_name,
    dim_product_child_level_1.product_child_name
    ) as product_child_name,
  case 
    when dim_product_child_level_2.product_child_quantity is not null
    then dim_product_child_level_2.product_child_quantity 
          * dim_product_child_level_1.product_child_quantity
          * fact_order.product_quantity
    else dim_product_child_level_1.product_child_quantity
          * fact_order.product_quantity
  end as product_child_quantity,
  coalesce(
    dim_product_child_level_2.product_child_code,
    dim_product_child_level_1.product_child_code
    ) as product_child_code,
  coalesce(
    dim_product_child_level_2.product_child_minimum_stock,
    dim_product_child_level_1.product_child_minimum_stock
    ) as product_child_minimum_stock,
  coalesce(
    dim_product_child_level_2.product_child_current_stock,
    dim_product_child_level_1.product_child_current_stock
    ) as product_child_current_stock,
  
from
  -- `star_schema.fact_order` fact_order
  {{ ref('fact_order') }} fact_order
left join
  -- `star_schema.dim_client` dim_client
  {{ ref('dim_client') }} dim_client
on
  fact_order.client_bling_id = dim_client.client_bling_id
left join
  -- `star_schema.dim_product` dim_product_child_level_1
  {{ ref('dim_product') }} dim_product_child_level_1
on
  fact_order.product_code = dim_product_child_level_1.product_code
left join
  -- `star_schema.dim_product` dim_product_child_level_2
  {{ ref('dim_product') }} dim_product_child_level_2
on
  dim_product_child_level_1.product_child_code = dim_product_child_level_2.product_code
where
  1=1
  and fact_order.order_status = 'Atendido'