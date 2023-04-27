WITH dim_product__source AS(
  SELECT *
  FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column AS(
  SELECT 
    stock_item_id AS product_key
    , stock_item_name AS product_name
    , brand AS product_brand
    , is_chiller_stock AS is_chiller_stock_boolean
    , quantity_per_outer
    , lead_time_days
    , unit_price
    , recommended_retail_price
    , typical_weight_per_unit
    , supplier_id AS supplier_key
    , color_id AS color_key
    , unit_package_id AS unit_package_key
    , outer_package_id AS outer_package_key
  FROM dim_product__source
)

, dim_product__cast_type AS(
  SELECT
    CAST(product_key AS INT) AS product_key  
    , CAST(product_name AS STRING) AS product_name
    , CAST(product_brand AS STRING) AS product_brand
    , CAST(is_chiller_stock_boolean AS BOOLEAN) AS is_chiller_stock_boolean
    , CAST(quantity_per_outer AS INT) AS quantity_per_outer
    , CAST(lead_time_days AS INT) AS lead_time_days
    , CAST(unit_price AS NUMERIC) AS unit_price
    , CAST(recommended_retail_price AS NUMERIC) AS recommended_retail_price
    , CAST(typical_weight_per_unit AS NUMERIC) AS typical_weight_per_unit
    , CAST(supplier_key AS INT) AS supplier_key
    , CAST(color_key AS INT) AS color_key
    , CAST(unit_package_key AS INT) AS unit_package_key
    , CAST(outer_package_key AS INT) AS outer_package_key
  FROM dim_product__rename_column
)

,dim_product__convert_boolean AS(
  SELECT
    *
    , CASE 
        WHEN is_chiller_stock_boolean IS TRUE THEN 'Chiller Stock'
        WHEN is_chiller_stock_boolean IS FALSE THEN 'Not Chiller Stock'
        WHEN is_chiller_stock_boolean IS NULL THEN 'Undefined'
        ELSE 'Invalid' 
      END AS is_chiller_stock
  from dim_product__cast_type
)


SELECT 
dim_product.product_key
, dim_product.product_name
, COALESCE(dim_product.product_brand, 'Undefined') AS product_brand
, COALESCE(dim_external_product.product_category_key, 0) AS product_category_key
, dim_product_category.product_category_name
, dim_product_category.parent_category_name
, dim_product.is_chiller_stock
, dim_product.quantity_per_outer
, dim_product.lead_time_days
, dim_product.unit_price
, dim_product.recommended_retail_price
, dim_product.typical_weight_per_unit
, dim_product.supplier_key
, COALESCE(dim_supplier.supplier_name, 'Invalid') AS supplier_name
, COALESCE(dim_supplier.supplier_category_name, 'Invalid') AS supplier_category_name
, COALESCE(dim_supplier.delivery_method_name, 'Invalid') AS supplier_delivery_method_name
, COALESCE(dim_supplier.delivery_city_name, 'Invalid') AS supplier_delivery_city_name
, COALESCE(dim_supplier.delivery_state_province_name, 'Invalid') AS supplier_delivery_state_province_name
, COALESCE(dim_supplier.delivery_country_name, 'Invalid') AS supplier_delivery_country_name
, COALESCE(dim_product.color_key, 0) AS color_key
, dim_color.color_name AS color_name
, dim_product.unit_package_key
, COALESCE(dim_unit_package_type.package_type_name, 'Invalid') AS unit_package_name
, dim_product.outer_package_key
, COALESCE(dim_outer_package_type.package_type_name, 'Invalid') AS outer_package_name
FROM dim_product__convert_boolean AS dim_product
LEFT JOIN {{ ref('dim_supplier')}} AS dim_supplier
  ON dim_product.supplier_key = dim_supplier.supplier_key
LEFT JOIN {{ ref('stg_dim_color')}} AS dim_color
  ON COALESCE(dim_product.color_key, 0) = dim_color.color_key
LEFT JOIN {{ ref('dim_package_type')}} AS dim_unit_package_type
  ON dim_product.unit_package_key = dim_unit_package_type.package_type_key
LEFT JOIN {{ ref('dim_package_type')}} AS dim_outer_package_type
  ON dim_product.outer_package_key = dim_outer_package_type.package_type_key
LEFT JOIN {{ ref('stg_dim_external_stock_item')}} AS dim_external_product
  ON dim_product.product_key = dim_external_product.product_key
LEFT JOIN {{ ref('stg_dim_external_product_category')}} AS dim_product_category 
  ON dim_external_product.product_category_key = dim_product_category.product_category_key