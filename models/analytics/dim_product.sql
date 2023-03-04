with dim_product__source as(
  select *
  from `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column as(
  select 
  stock_item_id as product_key
  ,stock_item_name as product_name
  ,brand as brand_name
  from dim_product__source
)