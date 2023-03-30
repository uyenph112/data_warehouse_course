WITH stg_dim_delivery_method__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, stg_dim_delivery_method__rename_column AS(
  SELECT
    delivery_method_id AS delivery_method_key
    , delivery_method_name AS delivery_method_name
  FROM stg_dim_delivery_method__source
)

, stg_dim_delivery_method__cast_type AS(
  SELECT
    CAST(delivery_method_key AS INT) AS delivery_method_key
    , CAST(delivery_method_name AS STRING) AS delivery_method_name
  FROM stg_dim_delivery_method__rename_column
)

SELECT
  delivery_method_key
  , delivery_method_name
FROM stg_dim_delivery_method__cast_type