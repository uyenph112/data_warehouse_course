SELECT
  person_key AS salesperson_person_key
  , person_name AS salesperson_person_name
  , preferred_person_name AS salesperson_preferred_person_name
FROM {{ ref('dim_person') }}
WHERE
  is_salesperson = 'Salesperson'
  OR person_key IN (0, -1)