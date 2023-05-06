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

, stg_dim_external_product_category__add_level AS(
  SELECT
    *
    , product_category_name AS level_1
    , 'Undefined' AS level_2
    , 'Undefined' AS level_3
    , 'Undefined' AS level_4
  FROM stg_dim_external_product_category__parent_category_name
  WHERE category_level = 1

  UNION ALL
 
  SELECT
    *
    , parent_category_name AS level_1
    , product_category_name AS level_2
    , 'Undefined' AS level_3
    , 'Undefined' AS level_4
  FROM stg_dim_external_product_category__parent_category_name
  WHERE category_level = 2

  UNION ALL

  SELECT 
    lv3.*
    , lv2.parent_category_name AS level_1
    , lv2.product_category_name AS level_2
    , lv3.product_category_name AS level_3
    , 'Undefined' AS level_4
  FROM stg_dim_external_product_category__parent_category_name AS lv3
  LEFT JOIN stg_dim_external_product_category__parent_category_name AS lv2 
    ON lv3.parent_category_key = lv2.product_category_key
  WHERE lv3.category_level = 3

  UNION ALL

  SELECT
    lv4.*
    , lv2.parent_category_name AS level_1
    , lv2.product_category_name AS level_2
    , lv3.product_category_name AS level_3 
    , lv4.product_category_name AS level_4
  FROM stg_dim_external_product_category__parent_category_name AS lv4
  LEFT JOIN stg_dim_external_product_category__parent_category_name AS lv3 
    ON lv4.parent_category_key = lv3.product_category_key
  LEFT JOIN stg_dim_external_product_category__parent_category_name AS lv2 
    ON lv3.parent_category_key = lv2.product_category_key
  WHERE lv4.category_level = 4

)

SELECT
  product_category_key
  , product_category_name
  , parent_category_key
  , parent_category_name
  , category_level
  , level_1
  , level_2
  , level_3
  , level_4
FROM stg_dim_external_product_category__add_level