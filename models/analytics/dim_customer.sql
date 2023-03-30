WITH dim_customer__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__customers`
)

, dim_customer__rename_column AS(
    SELECT 
      customer_id AS customer_key
      , customer_name AS customer_name
      , is_statement_sent AS is_statement_sent_boolean
      , is_on_credit_hold AS is_on_credit_hold_boolean
      , account_opened_date
      , payment_days
      , standard_discount_percentage AS standard_discount_pct
      , credit_limit 
      , customer_category_id AS customer_category_key
      , buying_group_id AS buying_group_key
      , delivery_method_id AS delivery_method_key
      , delivery_city_id AS delivery_city_key
      , postal_city_id AS postal_city_key
    from dim_customer__source
  )

, dim_customer__cast_type AS(
  SELECT
    CAST(customer_key AS INT) AS customer_key
    , CAST(customer_name AS STRING) AS customer_name
    , CAST(is_statement_sent_boolean AS BOOLEAN) AS is_statement_sent_boolean
    , CAST(is_on_credit_hold_boolean AS BOOLEAN) AS is_on_credit_hold_boolean
    , CAST(account_opened_date AS TIMESTAMP) AS account_opened_date
    , CAST(payment_days AS INT) AS payment_days
    , CAST(standard_discount_pct AS NUMERIC) AS standard_discount_pct
    , CAST(credit_limit AS NUMERIC) AS credit_limit
    , CAST(customer_category_key AS INT) AS customer_category_key
    , CAST(buying_group_key AS INT) AS buying_group_key
    , CAST(delivery_method_key AS INT) AS delivery_method_key
    , CAST(delivery_city_key AS INT) AS delivery_city_key
    , CAST(postal_city_key AS INT) AS postal_city_key
  FROM dim_customer__rename_column 
)

, dim_customer__convert_boolean AS(
  SELECT
    *
    , CASE 
        WHEN is_statement_sent_boolean IS TRUE THEN 'Statement Sent'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Statement Not Sent'
        WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_statement_sent
    , CASE 
        WHEN is_on_credit_hold_boolean IS TRUE THEN 'On Credit Hold'
        WHEN is_on_credit_hold_boolean IS FALSE THEN 'Not On Credit Hold'
        WHEN is_on_credit_hold_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid'
      END AS is_on_credit_hold    
  FROM dim_customer__cast_type    
)

SELECT
  dim_customer.customer_key
  , dim_customer.customer_name
  , dim_customer.is_statement_sent
  , dim_customer.is_on_credit_hold
  , dim_customer.account_opened_date
  , dim_customer.payment_days
  , dim_customer.standard_discount_pct
  , dim_customer.credit_limit
  , dim_customer.customer_category_key
  , COALESCE(dim_customer_cate.customer_category_name, 'Undefined') AS customer_category_name
  , dim_customer.buying_group_key
  , COALESCE(dim_buying_group.buying_group_name, 'Invalid') AS buying_group_name
  , dim_customer.delivery_method_key
  , COALESCE(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
  , dim_customer.delivery_city_key
  , COALESCE(dim_delivery_city.city_name, 'Invalid') AS delivery_city_name 
  , COALESCE(dim_delivery_city.state_province_name, 'Invalid') AS delivery_state_province_name
  , COALESCE(dim_delivery_city.country_name, 'Invalid') AS delivery_country_name
  , dim_customer.postal_city_key
  , COALESCE(dim_postal_city.city_name, 'Invalid') AS postal_city_name 
  , COALESCE(dim_postal_city.state_province_name, 'Invalid') AS postal_state_province_name
  , COALESCE(dim_postal_city.country_name, 'Invalid') AS postal_country_name
FROM dim_customer__convert_boolean AS dim_customer 
LEFT JOIN {{ ref('stg_dim_customer_category') }} AS dim_customer_cate
  ON dim_customer.customer_category_key = dim_customer_cate.customer_category_key
LEFT JOIN {{ ref('stg_dim_sales_buying_group') }} AS dim_buying_group 
 ON dim_customer.buying_group_key = dim_buying_group.buying_group_key
LEFT JOIN {{ ref('stg_dim_delivery_method') }} AS dim_delivery_method
  ON dim_customer.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_delivery_city 
  ON dim_customer.delivery_city_key = dim_delivery_city.city_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_postal_city
  ON dim_customer.postal_city_key = dim_postal_city.city_key