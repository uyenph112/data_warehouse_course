with stg_fact_sales_order__source as(
  select *
  from `vit-lam-data.wide_world_importers.sales__orders`
)

,stg_fact_sales_order__rename_column as(
  select 
  order_id as sales_order_key
  ,customer_id as customer_key
  ,picked_by_person_id as picked_by_person_key
  from stg_fact_sales_order__source
)

,stg_fact_sales_order__cast_type as(
  select
  cast(sales_order_key as int) as sales_order_key
  ,cast(customer_key as int) as customer_key
  ,cast(picked_by_person_key as int) as picked_by_person_key
  from stg_fact_sales_order__rename_column
)

select
sales_order_key
,customer_key
,picked_by_person_key
from stg_fact_sales_order__cast_type