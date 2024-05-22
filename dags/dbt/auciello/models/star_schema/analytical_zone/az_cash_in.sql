
{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'az_cash_in',
        enabled = True
    )
}}



with
cte_purchases as (
select
  date(session_datetime)                                  as session_date,
  format_date("%Y%m", session_datetime)                   as session_month,
  format_date("%B/%Y", session_datetime)                  as session_month_name,
  tray_order_id,
  round(sum(order_sales_value_item),2)                    as order_sales_value,
  round(sum(shipping_paid_value_item),2)                  as shipping_paid_value,
  round(sum(vindi_tax_item),2)                            as vindi_tax,

  round(sum(production_cost_with_invoice_value_items),2)  as production_cost_with_invoice_value,
  round(sum(reserve_value_items),2)                       as reserve_value,
  round(sum(salary_value_items),2)                        as salary_value,
  round(sum(cash_variable_value_items),2)                 as cash_variable_value,
from
  {{ ref('az_purchase_sessions') }}
where
  1=1
group by
  all
)
, cte_cash_in as (
    select
        session_date, 
        session_month, 
        session_month_name,
        tray_order_id, 
        1 as order_count,
        order_sales_value, 
        shipping_paid_value,
        vindi_tax,
        production_cost_with_invoice_value,
        reserve_value,
        salary_value,
        cash_variable_value
    from
        cte_purchases
    union all
    select
        date_trunc(session_date, MONTH) as session_date, 
        session_month, 
        session_month_name,
        'Subtotal' as tray_order_id, 
        count(distinct tray_order_id) as order_count,
        round(sum(order_sales_value),2) as order_sales_value, 
        round(sum(shipping_paid_value),2) as shipping_paid_value,
        round(sum(vindi_tax),2) as vindi_tax,
        round(sum(production_cost_with_invoice_value),2) as production_cost_with_invoice_value,
        round(sum(reserve_value),2) as reserve_value,
        round(sum(salary_value),2) as salary_value,
        round(sum(cash_variable_value),2) as cash_variable_value
    from
        cte_purchases
    group by 
        1, session_month, session_month_name
)
select
    *
from
    cte_cash_in
order by
    session_date desc

