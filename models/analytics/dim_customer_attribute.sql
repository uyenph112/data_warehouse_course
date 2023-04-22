WITH dim_customer_attribubte__source AS(
  SELECT
    customer_key
    , order_date
    , sales_order_key
    , gross_amount
  FROM {{ ref('fact_sales_order_line') }}
)

, dim_customer_attribubte__metric_caculated AS(
  SELECT
    customer_key
    , MIN(DATE_TRUNC(order_date, MONTH)) AS start_month
    , MAX(DATE_TRUNC(order_date, MONTH)) AS end_month
    , SUM(gross_amount) AS lifetime_sales_amount
    , COUNT(DISTINCT sales_order_key) AS lifetime_sales_orders
    , SUM(CASE 
            WHEN DATE_TRUNC(order_date, MONTH) = DATE '2016-05-01' THEN gross_amount
          END) AS LM_sales_amount
    , COUNT(DISTINCT 
            CASE
              WHEN DATE_TRUNC(order_date, MONTH) = DATE '2016-05-01' THEN sales_order_key
          END) AS LM_sales_orders
  FROM dim_customer_attribubte__source
    GROUP BY 1
)

, dim_customer_attribubte__percentile AS(
  SELECT
    *
    , PERCENT_RANK() OVER (ORDER BY lifetime_sales_amount) AS lifetime_percentile
    , PERCENT_RANK() OVER (ORDER BY LM_sales_amount) AS LM_percentile
  FROM dim_customer_attribubte__metric_caculated
)

, dim_customer_attribubte__segmentation AS(
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
  FROM dim_customer_attribubte__percentile
)

SELECT 
  customer_key
  , start_month
  , end_month
  , lifetime_sales_amount
  , lifetime_sales_orders
  , lifetime_percentile
  , lifetime_monetary_segment
  , LM_sales_amount
  , LM_sales_orders
  , LM_percentile
  , LM_monetary_segment
FROM dim_customer_attribubte__segmentation