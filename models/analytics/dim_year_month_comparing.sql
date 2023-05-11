SELECT
  CAST(date AS DATE) AS date
  , 'Last Year' AS comparing_type
  , CAST(CAST(date AS DATE) - INTERVAL '1' YEAR AS DATE) AS orginal_date
FROM {{ ref('dim_date')}}

UNION ALL

SELECT
  CAST(date AS DATE) AS date
  , 'This Year' AS comparing_type
  , CAST(date AS DATE) AS orginal_date
FROM {{ ref('dim_date')}}