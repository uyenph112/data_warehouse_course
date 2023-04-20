WITH fact_sales_performance__from_sales AS(
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , salesperson_person_key
    , SUM(gross_amount) AS actual_revenue
  FROM {{ ref ('fact_sales_order_line') }}
  GROUP BY 1, 2
)

, fact_sales_performance__from_target AS(
  SELECT
    year_month
    , salesperson_target_person_key AS salesperson_person_key
    , target_revenue
  FROM {{ ref ('stg_fact_salesperson_target') }}

)

, fact_sales_performance__sales_join_target AS(
  SELECT 
    COALESCE(fact_sales.year_month, fact_target.year_month) AS year_month
    , COALESCE(fact_sales.salesperson_person_key, fact_target.salesperson_person_key) AS salesperson_person_key
    , fact_sales.actual_revenue
    , fact_target.target_revenue
  FROM fact_sales_performance__from_sales AS fact_sales
  FULL JOIN fact_sales_performance__from_target AS fact_target 
    ON fact_sales.year_month = fact_target.year_month
      AND fact_sales.salesperson_person_key = fact_target.salesperson_person_key
)

, fact_sales_performance__achievement_pct AS(
  SELECT 
    *
    , actual_revenue / target_revenue AS achievement_pct
  FROM fact_sales_performance__sales_join_target
)

SELECT
  year_month
  , salesperson_person_key
  , actual_revenue
  , target_revenue
  , achievement_pct
  , CASE
      WHEN achievement_pct >= 0.8 THEN 'Achieved'
      ELSE 'Not Achieved'
    END AS is_achieved
FROM fact_sales_performance__achievement_pct