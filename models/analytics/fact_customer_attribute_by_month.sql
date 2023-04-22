WITH fact_customer_attribubte_by_month__source AS(
  SELECT
    order_date
    , customer_key
    , sales_order_key
    , gross_amount
  FROM {{ ref('fact_sales_order_line') }}
)

, fact_customer_attribubte_by_month__summary AS(
  SELECT
    DATE_TRUNC(order_date, MONTH) AS year_month
    , customer_key
    , SUM(gross_amount) AS sales_amount 
    , COUNT(DISTINCT sales_order_key) AS sales_orders
  FROM fact_customer_attribubte_by_month__source
  GROUP BY 1,2
)

, fact_customer_attribubte_by_month__generated_dimension AS(
  SELECT
    year_month
    , customer_key
  FROM 
    (SELECT DISTINCT year_month FROM {{ ref ('dim_date') }}) AS dim_date 
  CROSS JOIN
    (SELECT customer_key, start_month, end_month FROM {{ ref ('dim_customer_attribute') }}) AS fact_customer
  WHERE year_month BETWEEN start_month AND end_month
)

, fact_customer_attribute_by_month__metric_calculated AS(
  SELECT
    year_month
    , customer_key
    , COALESCE(sales_amount, 0) AS sales_amount
    , COALESCE (sales_orders, 0) AS sales_orders
    , COALESCE(SUM(COALESCE(sales_amount, 0)) OVER (PARTITION BY customer_key ORDER BY year_month), 0) AS lifetime_sales_amount
    , COALESCE(LAG(COALESCE(sales_amount, 0)) OVER (PARTITION BY customer_key ORDER BY year_month), 0) AS LM_sales_amount
    , COALESCE(SUM(COALESCE(sales_amount, 0)) OVER (PARTITION BY customer_key ORDER BY year_month ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING), 0) AS L12M_sales_amount
  FROM fact_customer_attribubte_by_month__generated_dimension AS generated_dimension
  LEFT JOIN fact_customer_attribubte_by_month__summary AS summary 
    USING(year_month, customer_key)
)

, fact_customer_attribute_by_month__percentile AS(
  SELECT
    *
    , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY lifetime_sales_amount) AS lifetime_percentile
    , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY LM_sales_amount) AS LM_percentile
    , PERCENT_RANK() OVER (PARTITION BY year_month ORDER BY L12M_sales_amount) AS L12M_percentile
  FROM fact_customer_attribute_by_month__metric_calculated
)

, fact_customer_attribute_by_month__segmentation AS(
  SELECT
    *
  , CASE 
        WHEN lifetime_percentile >=0 AND lifetime_percentile < 0.5 THEN 'Low'
        WHEN lifetime_percentile >=0.5 AND lifetime_percentile < 0.8 THEN 'Medium'
        WHEN lifetime_percentile >=0.8 THEN 'High'
      END AS lifetime_monetary_segment
    , CASE 
        WHEN LM_percentile >=0 AND LM_percentile < 0.5 THEN 'Low'
        WHEN LM_percentile >=0.5 AND LM_percentile < 0.8 THEN 'Medium'
        WHEN LM_percentile >=0.8 THEN 'High'
      END AS LM_monetary_segment
    , CASE 
        WHEN L12M_percentile >=0 AND L12M_percentile < 0.5 THEN 'Low'
        WHEN L12M_percentile >=0.5 AND L12M_percentile < 0.8 THEN 'Medium'
        WHEN L12M_percentile >=0.8 THEN 'High'
      END AS L12M_monetary_segment
  FROM fact_customer_attribute_by_month__percentile
)

SELECT
  year_month
  , customer_key
  , sales_amount
  , sales_orders
  , lifetime_sales_amount
  , lifetime_monetary_segment
  , LM_sales_amount
  , LM_monetary_segment
  , L12M_sales_amount
  , L12M_monetary_segment
FROM fact_customer_attribute_by_month__segmentation