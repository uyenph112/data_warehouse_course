WITH fact_sales_order_line__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

,fact_sales_order_line__rename_column AS(
  SELECT 
    order_line_id AS sales_order_line_key
    , order_id AS sales_order_key
    , stock_item_id AS product_key
    , package_type_id AS package_type_key
    , quantity
    , picked_quantity
    , unit_price
    , tax_rate
  FROM fact_sales_order_line__source
)

,fact_sales_order_line__cast_type AS(
  SELECT
    CAST(sales_order_line_key AS INT) AS sales_order_line_key
    , CAST(sales_order_key AS INT) AS sales_order_key
    , CAST(product_key AS INT) AS product_key
    , CAST(package_type_key AS INT) AS package_type_key
    , CAST(quantity AS INT) AS quantity
    , CAST(picked_quantity AS INT) AS picked_quantity
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(tax_rate AS NUMERIC) AS tax_rate
  FROM fact_sales_order_line__rename_column
)

,fact_sales_order_line__calculate_measure AS(
  SELECT 
    *
    , quantity * unit_price AS gross_amount
    , tax_rate * unit_price AS tax_amount
    , quantity * unit_price * (1 + tax_rate) AS customer_paid_amount
  FROM fact_sales_order_line__CAST_type
)

SELECT 
  fact_line.sales_order_line_key
  , fact_line.sales_order_key
  , fact_line.product_key
  , fact_line.package_type_key
  , fact_header.customer_key
  , fact_header.salesperson_person_key
  , fact_header.picked_by_person_key
  , fact_header.contact_person_key
  , fact_header.backorder_order_key
  , fact_header.is_undersupply_backordered_boolean
  , FARM_FINGERPRINT(CONCAT(fact_header.is_undersupply_backordered_boolean, ',' , fact_line.package_type_key)) AS sales_order_line_indicator_key
  , fact_header.order_date
  , fact_header.expected_delivery_date
  , fact_line.quantity
  , fact_line.unit_price
  , fact_line.gross_amount
  , fact_line.tax_amount
  , fact_line.customer_paid_amount
FROM fact_sales_order_line__calculate_measure AS fact_line
LEFT JOIN {{ ref('stg_fact_sales_order') }} AS fact_header
  ON fact_line.sales_order_key = fact_header.sales_order_key