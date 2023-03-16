WITH dim_date_generate AS(
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', INTERVAL 1 DAY)) AS d
)

SELECT
  FORMAT_DATE('%F', d) AS date
  , FORMAT_DATE('%A', d) AS day_of_week
  , FORMAT_DATE('%a', d) AS day_of_week_short
  , CASE
      WHEN EXTRACT(DAYOFWEEK FROM d) IN (1,7) THEN 'Weekend'
      ELSE 'Weekday' END
    AS is_weekday_or_weekend
  , DATE_TRUNC(d, MONTH) AS year_month
  , FORMAT_DATE('%B', d) AS month
  , DATE_TRUNC(d, YEAR) AS year 
  , EXTRACT(YEAR FROM d) AS year_number
FROM dim_date_generate