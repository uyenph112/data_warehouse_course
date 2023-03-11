with dim_customer__source as(
  select *
  from `vit-lam-data.wide_world_importers.sales__customers`
)

,dim_customer__rename_column as(
  select 
  customer_id as customer_key
  ,customer_name	as customer_name
  ,customer_category_id as customer_category_key
  ,buying_group_id as buying_group_key
  from dim_customer__source
)

,dim_customer__cast_type as(
  select
  cast(customer_key as int) as customer_key
  ,cast(customer_name as string) as customer_name
  ,cast(customer_category_key as int) as customer_category_key
  ,cast(buying_group_key as int) as buying_group_key
  from dim_customer__rename_column
)

select
customer_key
,customer_name
,customer_category_key
,buying_group_key
from dim_customer__cast_type