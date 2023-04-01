SELECT
  person_key AS picked_by_person_key
  , person_name AS picked_by_person_name
  , preferred_person_name AS preferred_picked_by_person_name
  , is_employee
FROM {{ ref('dim_person') }}