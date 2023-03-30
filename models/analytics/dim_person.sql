WITH dim_person__source AS(
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS(
  SELECT
    person_id AS person_key
    , full_name AS person_name
    , preferred_name AS preferred_person_name
    , is_system_user AS is_system_user_boolean
    , is_employee AS is_employee_boolean
    , is_salesperson AS is_salesperson_boolean
  FROM dim_person__source
)

, dim_person__cast_type AS(
  SELECT
    CAST(person_key AS INT) AS person_key
    , CAST(person_name AS STRING) AS person_name
    , CAST(preferred_person_name AS STRING) AS preferred_person_name
    , CAST(is_system_user_boolean AS BOOLEAN) AS is_system_user_boolean
    , CAST(is_employee_boolean AS BOOLEAN) AS is_employee_boolean
    , CAST(is_salesperson_boolean AS BOOLEAN) AS is_salesperson_boolean
  FROM dim_person__rename_column
)

, dim_person__convert_boolean AS(
  SELECT 
    *
    , CASE 
        WHEN is_system_user_boolean IS TRUE THEN 'System User'
        WHEN is_system_user_boolean IS FALSE THEN 'Not System User'
        WHEN is_system_user_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_system_user
    , CASE 
        WHEN is_employee_boolean IS TRUE THEN 'Employee'
        WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
        WHEN is_employee_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_employee
    , CASE 
        WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
        WHEN is_salesperson_boolean IS FALSE THEN 'Not Salesperson'
        WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_salesperson
  FROM dim_person__cast_type
)

, dim_person__add_undefined_record AS(
  SELECT
    person_key
    , person_name
    , preferred_person_name
    , is_system_user
    , is_employee
    , is_salesperson
  FROM dim_person__convert_boolean

UNION ALL 
  SELECT 
    0 AS person_key
    , 'Undefined' AS person_name
    , 'Undefined' AS preferred_person_name
    , 'Undefined' AS is_system_user
    , 'Undefined' AS is_employee
    , 'Undefined' AS is_salesperson

UNION ALL 
  SELECT 
    -1 AS person_key
    , 'Invalid' AS person_name
    , 'Invalid' AS preferred_person_name
    , 'Invalid' AS is_system_user
    , 'Invalid' AS is_employee
    , 'Invalid' AS is_salesperson
)

SELECT
  person_key
  , person_name
  , preferred_person_name
  , is_system_user
  , is_employee
  , is_salesperson
FROM dim_person__add_undefined_record