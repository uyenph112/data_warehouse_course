WITH stg_fact_salesperson_target__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.external__salesperson_target`
)

, stg_fact_salesperson_target__rename_column AS(
  SELECT
    salesperson_person_id AS salesperson_target_person_key
    , year_month AS year_month
    , target_revenue AS target_revenue
  FROM stg_fact_salesperson_target__source
)

, stg_fact_salesperson_target__cast_type AS(
  SELECT
    CAST(salesperson_target_person_key AS INT) AS salesperson_target_person_key
    , CAST(year_month AS DATE) AS year_month
    , CAST(target_revenue AS NUMERIC) AS target_revenue
  FROM stg_fact_salesperson_target__rename_column
)

SELECT
  salesperson_target_person_key
  , year_month
  , target_revenue
FROM stg_fact_salesperson_target__cast_type