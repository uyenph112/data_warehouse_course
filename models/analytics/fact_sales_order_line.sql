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

