with fact_sales_order_line__source as(
  select *
  from `vit-lam-data.wide_world_importers.sales__order_lines`
)

,fact_sales_order_line__rename_column as(
  select 
  order_line_id as sales_order_line_key
  ,order_id as sales_order_key
  ,stock_item_id as product_key
  ,quantity
  ,unit_price
  from fact_sales_order_line__source
)

,fact_sales_order_line__cast_type as(
  select
  cast(sales_order_line_key as int) as sales_order_line_key
  ,cast(sales_order_key as int) as sales_order_key
  ,cast(product_key as int) as product_key
  ,cast(quantity as int) as quantity
  ,cast(unit_price as numeric) as unit_price
  from fact_sales_order_line__rename_column
)

,fact_sales_order_line__calculate_measure as(
  select *
  ,quantity * unit_price as gross_amount
  from fact_sales_order_line__cast_type
)

select 
fact_line.sales_order_line_key
,fact_line.sales_order_key
,fact_line.product_key
,fact_header.customer_key
,COALESCE(fact_header.picked_by_person_key, -1) AS picked_by_person_key
,fact_header.order_date
,fact_line.quantity
,fact_line.unit_price
,fact_line.gross_amount
from fact_sales_order_line__calculate_measure as fact_line
left join {{ ref('stg_fact_sales_order') }} as fact_header
on fact_line.sales_order_key = fact_header.sales_order_key