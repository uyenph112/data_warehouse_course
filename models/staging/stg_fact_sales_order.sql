WITH stg_fact_sales_order__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, stg_fact_sales_order__rename_column AS(
    SELECT
      order_id AS sales_order_key
      , customer_id AS customer_key
      , salesperson_person_id AS salesperson_person_key
      , picked_by_person_id AS picked_by_person_key
      , contact_person_id AS contact_person_key
      , backorder_order_id AS backorder_order_key
      , is_undersupply_backordered AS is_undersupply_backordered_boolean
      , order_date
      , expected_delivery_date
    FROM stg_fact_sales_order__source
)

, stg_fact_sales_order__cast_type AS(
    SELECT
      CAST(sales_order_key AS INT) AS sales_order_key
      , CAST(customer_key AS INT) AS customer_key
      , CAST(salesperson_person_key AS INT) AS salesperson_person_key
      , CAST(picked_by_person_key AS INT) AS picked_by_person_key
      , CAST(contact_person_key AS INT) AS contact_person_key
      , CAST(backorder_order_key AS INT) AS backorder_order_key
      , CAST(is_undersupply_backordered_boolean AS BOOLEAN) AS is_undersupply_backordered_boolean
      , CAST(order_date AS DATE) AS order_date
      , CAST(expected_delivery_date AS DATE) AS expected_delivery_date
    FROM stg_fact_sales_order__rename_column
)

SELECT
sales_order_key
, customer_key
, salesperson_person_key
, COALESCE(picked_by_person_key, 0) AS picked_by_person_key
, contact_person_key
, backorder_order_key
, is_undersupply_backordered_boolean
, order_date
, expected_delivery_date
FROM stg_fact_sales_order__cast_type