with fact_sales_order_lines__source as(
  select *
  from `vit-lam-data.wide_world_importers.sales__order_lines`
)

,fact_sales_order_lines__rename_column as(
  select 
  order_line_id as sales_order_line_key
  ,stock_item_id as product_key
  ,quantity
  ,unit_price
  from fact_sales_order_lines__source
)

,fact_sales_order_lines__cast_type as(
  select
  cast(sales_order_line_key as int) as sales_order_line_key
  ,cast(product_key as int) as product_key
  ,cast(quantity as int) as quantity
  ,cast(unit_price as numeric) as unit_price
  from fact_sales_order_lines__rename_column
)


select 
sales_order_line_key
,product_key
,quantity
,unit_price
,quantity * unit_price as gross_amount
from fact_sales_order_lines__cast_type