WITH stg_dim_state_province__source AS(
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__state_provinces`
)

, stg_dim_state_province__rename_column AS(
    SELECT
      state_province_id AS state_province_key
      , state_province_name AS state_province_name
      , sales_territory
      , latest_recorded_population
      , country_id AS country_key
    FROM stg_dim_state_province__source
)

, stg_dim_state_province__cast_type AS(
    SELECT
      CAST(state_province_key AS INT) AS state_province_key
      , CAST(state_province_name AS STRING) AS state_province_name
      , CAST(sales_territory AS STRING) AS sales_territory
      , CAST(latest_recorded_population AS BIGINT) AS latest_recorded_population
      , CAST(country_key AS INT) AS country_key
    FROM stg_dim_state_province__rename_column 
)

SELECT
  dim_state.state_province_key
  , dim_state.state_province_name
  , dim_state.sales_territory
  , dim_state.latest_recorded_population
  , dim_state.country_key
  , COALESCE(dim_country.country_name, 'Invalid') AS country_name
FROM stg_dim_state_province__cast_type AS dim_state 
LEFT JOIN {{ ref('stg_dim_country') }} AS dim_country
  ON dim_state.country_key = dim_country.country_key