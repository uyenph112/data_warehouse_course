with dim_customer__source as(
  select *
  from `vit-lam-data.wide_world_importers.sales__customers`
)

,dim_customer__rename_column as(
  select 
  customer_id as customer_key
  customer_name	as customer_name
  from dim_customer__source
)