{% snapshot dim_product_snapshot %}

{{
    config(
      target_database='auciello-design',
      target_schema='snapshots',
      unique_key="product_family_id",
      alias = 'dim_product',
      strategy='check',
      check_cols=[
        'product_name',
        'product_code',
        'category_name',
        'tray_product_id',

        'sales_value',
        'cost_value',
        'product_child_id', 
        'product_child_quantity',
        'product_child_code',
        'product_child_name',
        'product_child_sales_value',
        'product_child_cost_value',
        'product_child_package_cost_value',
        'product_child_crossdocking'
      ],
      enabled=true
    )
}}

select distinct 
    bling_product_id||'-'||coalesce(cast(product_child_id as string),'') as product_family_id,
    * 
from 
    {{ ref('dim_product') }}



{% endsnapshot %}