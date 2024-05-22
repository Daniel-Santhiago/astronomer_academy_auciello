{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'fact_product_sales_value'
    )
}}

with
cte_bling_produtos as (
  select
    tray_product_id,
    product_name,
    category_id,
    case
      when dbt_valid_from = '2024-03-18 01:03:47.119815 UTC' 
      then datetime('2023-01-01 03:00:00','America/Sao_Paulo')
      when dbt_valid_from = '2024-03-31 19:48:15.454584 UTC' 
      then datetime('2023-01-01 03:00:00','America/Sao_Paulo')
      when dbt_valid_from = '2024-04-01 01:05:07.918120 UTC' 
      then datetime('2023-01-01 03:00:00','America/Sao_Paulo')
      else datetime(dbt_valid_from,'America/Sao_Paulo')
    end                                                               as dbt_valid_from,
    coalesce(
      timestamp_sub(date_trunc(datetime(dbt_valid_to), day), interval 1 second), 
      current_datetime('America/Sao_Paulo')
    )                                                                 as dbt_valid_to,
    product_child_name,
    product_child_cost_value,
    product_child_package_cost_value,
    product_child_quantity

  from 
    -- `auciello-design.snapshots.dim_product` p
    {{ ref('dim_product_snapshot') }} p
  where
    1=1
    and tray_product_id is not null 
    and tray_product_id != ''
  order by
    tray_product_id desc
)
, cte_daily_produtos AS (
  SELECT
    tray_product_id,
    product_name,
    category_id,
    DATE(DATE_ADD(dbt_valid_from, INTERVAL offset DAY)) AS base_date,
    product_child_name,
    product_child_cost_value,
    product_child_package_cost_value,
    product_child_quantity
    
  FROM
    cte_bling_produtos p,
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(dbt_valid_to, dbt_valid_from, DAY))) AS offset
)
, cte_mapping_categoria as (
  select 
    id_categoria,
    descricao,
    lucro_desejado,
    perc_reserva, 
    perc_caixa,
    perc_salario,
    case 
      when dbt_valid_from = '2024-01-04 23:41:06.666463 UTC' 
      then datetime('2023-01-01 03:00:00','America/Sao_Paulo')
      else datetime(dbt_valid_from,'America/Sao_Paulo')
    end                                                               as  dbt_valid_from,
    coalesce(
      timestamp_sub(date_trunc(datetime(dbt_valid_to), day), interval 1 second), 
      current_datetime('America/Sao_Paulo')
    )                                                                 as dbt_valid_to,
  from 
    -- `auciello-design.snapshots.mapeamento_categorias` mc
    -- {{ source('snapshots','mapeamento_categorias') }} mc
    {{ ref('bling_sheets_mapeamento_categorias_snapshot') }} mc
  where
    1=1
)
, cte_daily_mapping_categorias as (
  select
    DATE(DATE_ADD(dbt_valid_from, INTERVAL offset DAY)) AS base_date,
    id_categoria,
    descricao,
    lucro_desejado,
    perc_reserva, 
    perc_caixa,
    perc_salario,
  from
    cte_mapping_categoria p,
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(dbt_valid_to, dbt_valid_from, DAY))) AS offset
)
, cte_cost as (
  select
    tray_product_id,
    product_name,
    categoria.descricao                                                           as category_name,
    produtos.base_date                                                            as base_date,
    categoria.lucro_desejado                                                      as target_profit_percentage,
    categoria.perc_reserva                                                        as reserve_percentage,
    categoria.perc_caixa                                                          as cash_percentage,
    categoria.perc_salario                                                        as salary_percentage,
    round(sum(product_child_cost_value * product_child_quantity),2)               as production_cost_value,
    round(sum(product_child_package_cost_value * product_child_quantity),2)       as production_package_cost_value,
  FROM
    cte_daily_produtos produtos
  inner join
    cte_daily_mapping_categorias categoria
  on
    cast(produtos.category_id as string) = cast(categoria.id_categoria as string)
    and produtos.base_date = categoria.base_date
  where 
    1=1
    and lucro_desejado is not null
  group by
    all
  order by
    base_date desc
)
, cte_valores as (
  select
    base_date,
    tray_product_id,
    category_name,
    product_name,
    production_cost_value,
    production_package_cost_value,
    case 
      when target_profit_percentage != 0.0
      then ROUND((production_cost_value * 0.04) ,2) 
      else 0.0
    end as invoice_cost_value,
    case 
      when target_profit_percentage != 0.0
      then CEIL((production_cost_value * 1.04)) 
      else production_cost_value
    end as production_cost_with_invoice_value,

    target_profit_percentage,
    reserve_percentage,
    cash_percentage,
    salary_percentage,
    case 
      when target_profit_percentage != 0.0  
      then ROUND( (CEIL(production_cost_value * 1.04) / ((1/(target_profit_percentage))-1)) ,2) 
      else 0.0
    end as lucro_bruto,
    case 
      when target_profit_percentage != 0.0 
      then CEIL(( ((production_cost_value * 1.04) / ((1/(target_profit_percentage))-1)) * reserve_percentage ) ) 
      else production_cost_value * reserve_percentage
    end as reserve_value,
    case 
      when target_profit_percentage != 0.0 
      then FLOOR(( ((production_cost_value * 1.04) / ((1/(target_profit_percentage))-1)) * cash_percentage ) ) 
      else production_cost_value * cash_percentage
    end as cash_value,
    case
      when target_profit_percentage != 0.0
      then CEIL(( ((production_cost_value * 1.04) / ((1/(target_profit_percentage))-1)) * salary_percentage ) ) 
      else production_cost_value * salary_percentage
    end as salary_value,
  from 
    cte_cost
)
select 
  *,
  (production_cost_with_invoice_value + reserve_value + cash_value + salary_value) as product_sales_value
from 
  cte_valores
where 
  1=1
