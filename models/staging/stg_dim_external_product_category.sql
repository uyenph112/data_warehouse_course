WITH stg_dim_external_product_category__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__categories`
)

, stg_dim_external_product_category__rename_column AS(
  SELECT
    category_id AS product_category_key
    , category_name AS product_category_name 
    , parent_category_id AS parent_category_key
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
  FROM stg_dim_external_product_category__cast_type

  UNION ALL
  SELECT
    0 AS product_category_key
    , 'Undefined' AS product_category_name
    , 0 AS parent_category_key
    , 0 AS category_level
)

, stg_dim_external_product_category__parent_category_name AS(
  SELECT
    product_category.product_category_key 
    , product_category.product_category_name
    , product_category.parent_category_key
    , parent_category.product_category_name AS parent_category_name
    , product_category.category_level
  FROM stg_dim_external_product_category__undefined_record AS product_category
  LEFT JOIN stg_dim_external_product_category__undefined_record AS parent_category
    ON product_category.parent_category_key = parent_category.product_category_key
)

SELECT
  product_category_key
  , product_category_name
  , parent_category_key
  , parent_category_name
  , category_level
FROM stg_dim_external_product_category__parent_category_name