{{
    config(
        materialized = 'table',
        schema= 'star_schema',
        alias = 'dim_product'
    )
}}

select
  produtos.id                         as bling_product_id,
  produtos.idfabricante               as supplier_id,
  produtos.categoria_id               as category_id,
  produtos.descricao                  as product_name,
  produtos.codigo                     as product_code,
  produtos.categoria_descricao        as category_name,
  produtos.observacoes                as tray_product_id,

  produtos.preco                      as sales_value,
  produtos.precocusto                 as cost_value,
  -- case 
  --   when produtos.codigo like 'EMB%'
  --   then produtos.precocusto
  --   else 0
  -- end                                 as package_cost_value,
  componente_produto.id               as product_child_id,
  pec.componente_quantidade           as product_child_quantity,
  
  componente_produto.codigo           as product_child_code,
  componente_produto.descricao        as product_child_name,
  componente_produto.preco            as product_child_sales_value,
  componente_produto.precocusto       as product_child_cost_value,

  case 
    when componente_produto.codigo like 'EMB%'
    then componente_produto.precocusto
    else 0
  end                                 as product_child_package_cost_value,
  componente_produto.crossdocking     as product_child_crossdocking,
  componente_produto.estoqueminimo    as product_child_minimum_stock,
  componente_estoque.estoqueAtual     as product_child_current_stock,  

from
  -- `auciello-design`.`kondado`.`bling_produtos` produtos
  {{ source('kondado','bling_produtos') }} produtos
left join
  -- `auciello-design`.`kondado`.`bling_produtos_estruturacomponente` pec
  {{ source('kondado','bling_produtos_estruturacomponente') }} pec
on
  produtos.id = pec.produto_id
left join 
  -- `auciello-design`.`kondado`.`bling_produtos` componente_produto
  {{ source('kondado','bling_produtos') }} componente_produto
ON 
  pec.componente_codigo = componente_produto.codigo
left join
  -- `auciello-design`.`kondado`.`bling_produtosestoque` componente_estoque
  {{ source('kondado','bling_produtosestoque') }} componente_estoque
on
  componente_produto.id = componente_estoque.produto_id