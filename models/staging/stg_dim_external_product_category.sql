WITH stg_dim_external_product_category__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__categories`
)

,stg_dim_external_product_category__rename_column AS(
  SELECT
    category_id AS product_category_key
    , category_name AS product_category_name 
    , parent_category AS parent_category_key
    , category_level
  FROM stg_dim_external_product_category__source
)

, stg_dim_external_product_category__cast_type AS(
  SELECT
    CAST(product_category_key AS INT) AS product_category_key 
    , CAST(product_category_name AS STRING) AS product_category_name
    , CAST(parent_category_key AS INT) AS parent_category_key
    , category_level
  FROM stg_dim_external_product_category__rename_column
)

, stg_dim_external_product_category__undefined_record AS(
  SELECT
    product_category_key
    , product_category_name
    , parent_category_key
    , category_level
  FROM stg_dim_color__cast_type

  UNION ALL
  SELECT
    0 AS product_category_key
    , 'Undefined' AS product_category_name
    , 0 AS parent_category_key
    , 0 AS category_level
)

SELECT
  product_category_key
  , product_category_name
  , parent_category_key
  , category_level
FROM stg_dim_external_product_category__undefined_record