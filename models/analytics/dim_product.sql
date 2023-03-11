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
  ,is_chiller_stock 
  from dim_product__source
)

,dim_product__cast_type as(
  select
  cast(product_key as int) as product_key
  ,cast(supplier_key as int) as supplier_key
  ,cast(product_name as string) as product_name
  ,cast(brand_name as string) as brand_name
  ,cast(is_chiller_stock as boolean) as is_chiller_stock
  from dim_product__rename_column
)

SELECT 
dim_product.product_key
,dim_product.supplier_key
,dim_product.product_name
,dim_supplier.supplier_name
,dim_product.brand_name
,dim_product.is_chiller_stock
FROM dim_product__cast_type AS dim_product
LEFT JOIN {{ ref('dim_supplier')}} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key