WITH stg_dim_country__source AS(
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__countries`
)

, stg_dim_country__rename_column AS(
    SELECT
      country_id AS country_key
      , country_name AS country_name
      , iso_alpha_3_code
      , iso_numeric_code
      , continent
      , region
      , subregion
    FROM stg_dim_country__source
)

, stg_dim_country__cast_type AS(
    SELECT
      CAST(country_key AS INT) AS country_key
      , CAST(country_name AS STRING) AS country_name
      , CAST(continent AS STRING) AS continent
      , CAST(region AS STRING) AS region
      , CAST(subregion AS STRING) AS subregion
      , CAST(iso_alpha_3_code AS STRING) AS iso_alpha_3_code
      , CAST(iso_numeric_code AS INT) AS iso_numeric_code
    FROM stg_dim_country__rename_column 
)

SELECT
  country_key
  , country_name
  , continent
  , region
  , subregion
  , COALESCE(iso_alpha_3_code, 'Undefined') AS iso_alpha_3_code
  , COALESCE(iso_numeric_code, 0) AS iso_numeric_code
FROM stg_dim_country__cast_type