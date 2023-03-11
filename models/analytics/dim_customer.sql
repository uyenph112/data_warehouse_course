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
dim_customer.customer_key
,dim_customer.customer_name
,dim_customer.customer_category_key
,dim_customer_cate.customer_category_name
,dim_customer.buying_group_key
,dim_buying_group.buying_group_name
from dim_customer__cast_type AS dim_customer 
left join {{ ref('stg_dim_customer_category') }} dim_customer_cate
  on dim_customer.customer_category_key = dim_customer_cate.customer_category_key
left join {{ ref('stg_dim_sales_buying_group') }} dim_buying_group 
 on dim_customer.buying_group_key = dim_buying_group.buying_group_key