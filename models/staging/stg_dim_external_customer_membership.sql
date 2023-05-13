WITH stg_dim_external_customer_membership__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__customer_membership`
)

,stg_dim_external_customer_membership__rename_column AS(
  SELECT
    customer_id AS customer_key
    , membership 
    , begin_effective_date
    , end_effective_date
  FROM stg_dim_external_customer_membership__source
)

, stg_dim_external_customer_membership__cast_type AS(
  SELECT
    CAST(customer_key AS INT) AS customer_key
    , CAST(membership AS STRING) AS membership
    , CAST(begin_effective_date AS DATE) AS begin_effective_date
    , CAST(end_effective_date AS DATE) AS end_effective_date
  FROM stg_dim_external_customer_membership__rename_column
)

SELECT
  customer_key
  , membership
  , begin_effective_date
  , end_effective_date
FROM stg_dim_external_customer_membership__cast_type