WITH stg_dim_city__source AS(
    SELECT *
    FROM `vit-lam-data.wide_world_importers.application__cities`
)

, stg_dim_city__rename_column AS(
    SELECT
      city_id AS city_key
      , city_name AS city_name
      , latest_recorded_population AS city_latest_recorded_population
      , state_province_id AS state_province_key
    FROM stg_dim_city__source
)

, stg_dim_city__cast_type AS(
    SELECT
      CAST(city_key AS INT) AS city_key
      , CAST(city_name AS STRING) AS city_name
      , CAST(city_latest_recorded_population AS BIGINT) AS city_latest_recorded_population
      , CAST(state_province_key AS INT) AS state_province_key
    FROM stg_dim_city__rename_column 
)

SELECT
  dim_city.city_key
  , dim_city.city_name
  , dim_city.city_latest_recorded_population
  , dim_city.state_province_key
  , COALESCE(dim_state.state_province_name, 'Invalid') AS state_province_name
  , COALESCE(dim_state.country_name, 'Invalid') AS country_name
FROM stg_dim_city__cast_type AS dim_city  
LEFT JOIN {{ ref('stg_dim_state_province') }} AS dim_state
  ON dim_city.state_province_key = dim_state.state_province_key