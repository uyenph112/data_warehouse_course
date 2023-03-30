WITH stg_dim_package_type__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, stg_dim_package_type__rename_column AS(
  SELECT
    package_type_id AS package_type_key
    , package_type_name AS package_type_name
  FROM stg_dim_package_type__source
)

, stg_dim_package_type__cast_type AS(
  SELECT
    CAST(package_type_key AS INT) AS package_type_key
    , CAST(package_type_name AS STRING) AS package_type_name
  FROM stg_dim_package_type__rename_column
)

SELECT
  package_type_key
  , package_type_name
FROM stg_dim_package_type__cast_type