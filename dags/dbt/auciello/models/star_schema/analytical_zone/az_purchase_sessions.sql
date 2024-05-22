{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_purchase_sessions'
    )
}}



with 
cte_purchase_event as (
select distinct
  fact_session.session_id,
  fact_session.session_datetime,
  fact_session.session_date,

  fact_session.channel,
  fact_session.traffic_name,
  fact_session.source,
  fact_session.medium,
  fact_session.campaign_name,
  fact_session.content,
  fact_session.user_pseudo_id,
  fact_order.client_bling_id,
  dim_client.client_name,
  dim_client.client_phone,
  dim_client.client_mail,
  fact_session.tray_order_id,
  fact_session.tray_product_id,
  fact_session.tray_product_name,
  fact_session.tray_product_quantity,
  
  fact_session.tray_product_original_price,


  fact_order.order_sales_value,
  fact_order.shipping_value,
  fact_order.shipping_paid_value,
  fact_order.order_discount_value,

  fact_order.vindi_tax,

  (sum(tray_product_original_price * tray_product_quantity) 
  over(partition by tray_order_id, fact_session.tray_product_id))
  / (sum(tray_product_original_price * tray_product_quantity) 
    over(partition by tray_order_id)) 
  as order_item_percent,

  fact_product_sales_value.production_cost_with_invoice_value                               as production_cost_with_invoice_value,
  fact_product_sales_value.production_package_cost_value                                    as production_package_cost_value,
  
  fact_product_sales_value.reserve_value                                                    as reserve_value,
  fact_product_sales_value.cash_value                                                       as cash_value,
  fact_product_sales_value.salary_value                                                     as salary_value,
  fact_product_sales_value.production_cost_with_invoice_value * tray_product_quantity       as production_cost_with_invoice_value_items,
  fact_product_sales_value.reserve_value * tray_product_quantity                            as reserve_value_items,
  fact_product_sales_value.cash_value * tray_product_quantity                               as cash_value_items,
  fact_product_sales_value.salary_value * tray_product_quantity                             as salary_value_items,

  (fact_product_sales_value.production_cost_with_invoice_value * tray_product_quantity)
  + (fact_product_sales_value.reserve_value * tray_product_quantity)
  + (fact_product_sales_value.salary_value * tray_product_quantity)
            as revenue_fix_item_value,

from
  {{ ref('fact_session_website') }} fact_session
inner join 
  {{ ref('dim_product') }} dim_product
using(tray_product_id)
inner join
  {{ ref('fact_order') }} fact_order
using(tray_order_id, product_code)
inner join
  {{ ref('fact_product_sales_value') }} fact_product_sales_value
on
  fact_session.tray_product_id = fact_product_sales_value.tray_product_id
  and fact_session.session_date = fact_product_sales_value.base_date
left join
  {{ ref('dim_client') }} dim_client
using(client_bling_id)
where
  1=1
  and event_name = 'purchase'
  and upper(order_status) NOT LIKE '%CANCELADO%'

)
, cte_aux_package_calculation as (
select
  sum(tray_product_quantity) over(partition by tray_order_id) 
                                                        as tray_order_total_quantity,
  round(
    sum(production_package_cost_value*tray_product_quantity) over(partition by tray_order_id) 
  ,2)
                                                        as tray_order_total_package_cost_value,                                                      
  max(production_package_cost_value) over(partition by tray_order_id) 
                                                        as tray_order_max_package_cost_value,
  case 
    when sum(tray_product_quantity) over(partition by tray_order_id) = 1
    then 1
    else ceil(
          sum(tray_product_quantity) over(partition by tray_order_id) /2
          )                                                     
  end as tray_order_packages_number,
  *
from 
  cte_purchase_event p
where
  1=1

)
, cte_package_calculation as (
select
  round(
    tray_order_total_package_cost_value 
  - (tray_order_packages_number * tray_order_max_package_cost_value)
  ,2
  )                                           as tray_order_extra_package_cost_value,
  *
from
  cte_aux_package_calculation
where
  1=1
)

select 
  * 
  except(
      production_cost_with_invoice_value,
      reserve_value,
      cash_value,
      salary_value,
      production_cost_with_invoice_value_items,
      reserve_value_items,
      cash_value_items,
      salary_value_items,
      tray_order_extra_package_cost_value,
      tray_order_total_quantity,
      tray_order_total_package_cost_value,
      tray_order_max_package_cost_value,
      tray_order_packages_number
  ),

  ROUND((order_sales_value * order_item_percent),2)     as order_sales_value_item,

  ROUND((production_cost_with_invoice_value_items 
    - (tray_order_extra_package_cost_value * order_item_percent))
    ,2) 
                                                        as production_cost_with_invoice_value_items,
  ROUND((tray_order_extra_package_cost_value * order_item_percent),2)
                                                        as extra_package_cost_value_items,                                                 
  reserve_value_items,
  cash_value_items,
  salary_value_items,

  ROUND((shipping_value * order_item_percent),2)        as shipping_value_item,
  ROUND((shipping_paid_value * order_item_percent),2)   as shipping_paid_value_item,
  ROUND((vindi_tax * order_item_percent),2)             as vindi_tax_item,
  -- ROUND((ajuste_preco * item_perc_purchase),2) as ajuste_preco_item,
  -- (cupom_desconto_valor * item_perc_purchase) as cupom_desconto_item,
  (order_discount_value * order_item_percent)           as order_discount_value_item,

  ROUND(
  (order_sales_value * order_item_percent) 
  - production_cost_with_invoice_value_items
  - reserve_value_items
  - salary_value_items
  - (shipping_paid_value * order_item_percent)
  - (vindi_tax * order_item_percent)
  + (tray_order_extra_package_cost_value * order_item_percent)

-- + (ajuste_preco * item_perc_purchase)
-- + (cupom_desconto_valor * item_perc_purchase)
  ,2)
  as cash_variable_value_items,


from 
  cte_package_calculation p
where
  1=1





-- with 
-- cte_purchase_event as (
-- select distinct
--   fact_session.session_id,
--   fact_session.session_datetime,
--   fact_session.session_date,

--   fact_session.channel,
--   fact_session.traffic_name,
--   fact_session.source,
--   fact_session.medium,
--   fact_session.campaign_name,
--   fact_session.content,
--   fact_session.user_pseudo_id,

--   fact_session.tray_order_id,
--   fact_session.tray_product_id,
--   fact_session.tray_product_name,
--   fact_session.tray_product_quantity,

--   fact_session.tray_product_original_price,


--   fact_order.order_sales_value,
--   fact_order.shipping_value,
--   fact_order.shipping_paid_value,
--   fact_order.order_discount_value,

--   fact_order.vindi_tax,

--   (sum(tray_product_original_price * tray_product_quantity) 
--   over(partition by tray_order_id, fact_session.tray_product_id))
--   / (sum(tray_product_original_price * tray_product_quantity) 
--     over(partition by tray_order_id)) 
--   as order_item_percent,

--   fact_product_sales_value.production_cost_with_invoice_value                               as production_cost_with_invoice_value,
--   fact_product_sales_value.reserve_value                                                    as reserve_value,
--   fact_product_sales_value.cash_value                                                       as cash_value,
--   fact_product_sales_value.salary_value                                                     as salary_value,
--   fact_product_sales_value.production_cost_with_invoice_value * tray_product_quantity       as production_cost_with_invoice_value_items,
--   fact_product_sales_value.reserve_value * tray_product_quantity                            as reserve_value_items,
--   fact_product_sales_value.cash_value * tray_product_quantity                               as cash_value_items,
--   fact_product_sales_value.salary_value * tray_product_quantity                             as salary_value_items,

--   (fact_product_sales_value.production_cost_with_invoice_value * tray_product_quantity)
--   + (fact_product_sales_value.reserve_value * tray_product_quantity)
--   + (fact_product_sales_value.salary_value * tray_product_quantity)
--             as revenue_fix_item_value,

-- from
--   {{ ref('fact_session_website') }} fact_session
-- inner join 
--   {{ ref('dim_product') }} dim_product
-- using(tray_product_id)
-- inner join
--   {{ ref('fact_order') }} fact_order
-- using(tray_order_id, product_code)
-- inner join
--   {{ ref('fact_product_sales_value') }} fact_product_sales_value
-- on
--   fact_session.tray_product_id = fact_product_sales_value.tray_product_id
--   and fact_session.session_date = fact_product_sales_value.base_date
-- where
--   1=1
--   and event_name = 'purchase'
--   and upper(order_status) NOT LIKE '%CANCELADO%'

-- )
-- select 
--   * 
--   except(
--       production_cost_with_invoice_value,
--       reserve_value,
--       cash_value,
--       salary_value,
--       production_cost_with_invoice_value_items,
--       reserve_value_items,
--       cash_value_items,
--       salary_value_items
--   ),

--   ROUND((order_sales_value * order_item_percent),2)  as order_sales_value_item,

--   production_cost_with_invoice_value_items,
--   reserve_value_items,
--   cash_value_items,
--   salary_value_items,

--   ROUND((shipping_value * order_item_percent),2)        as shipping_value_item,
--   ROUND((shipping_paid_value * order_item_percent),2)   as shipping_paid_value_item,
--   ROUND((vindi_tax * order_item_percent),2)             as vindi_tax_item,
--   -- ROUND((ajuste_preco * item_perc_purchase),2) as ajuste_preco_item,
--   -- (cupom_desconto_valor * item_perc_purchase) as cupom_desconto_item,
--   (order_discount_value * order_item_percent)           as order_discount_value_item,

--   ROUND(
--   (order_sales_value * order_item_percent) 
--   - production_cost_with_invoice_value_items
--   - reserve_value_items
--   - salary_value_items
--   - (shipping_paid_value * order_item_percent)
--   - (vindi_tax * order_item_percent)
-- -- + (ajuste_preco * item_perc_purchase)
-- -- + (cupom_desconto_valor * item_perc_purchase)
--   ,2)
--   as cash_variable_value_items

-- from 
--   cte_purchase_event p
-- where
--   1=1

