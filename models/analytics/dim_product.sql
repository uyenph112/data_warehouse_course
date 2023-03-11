with dim_product__source as(
  select *
  from `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column as(
  select 
  stock_item_id as product_key
  ,supplier_id as supplier_key
  ,stock_item_name as product_name
  ,brand as brand_name
  from dim_product__source
)

,dim_product__cast_type as(
  select
  cast(product_key as int) as product_key
  ,cast(supplier_key as int) as supplier_key
  ,cast(product_name as string) as product_name
  ,cast(brand_name as string) as brand_name
  from dim_product__rename_column
)

SELECT 
product_key
,supplier_key
,product_name
,brand_name
FROM dim_product__cast_type AS dim_product 