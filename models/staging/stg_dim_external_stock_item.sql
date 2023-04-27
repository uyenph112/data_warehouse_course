WITH stg_dim_external_stock_item__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__stock_item`
)

,stg_dim_external_stock_item__rename_column AS(
  SELECT
    stock_item_id AS product_key
    , category_id AS product_category_key
  FROM stg_dim_external_stock_item__source
)

, stg_dim_external_stock_item__cast_type AS(
  SELECT
    CAST(product_key AS INT) AS product_key
    , CAST(product_category_key AS INT) AS product_category_key
  FROM stg_dim_external_stock_item__rename_column
)

SELECT
  product_key
  , product_category_key
FROM stg_dim_external_stock_item__cast_type