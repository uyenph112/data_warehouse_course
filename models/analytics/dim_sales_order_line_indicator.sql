WITH dim_is_under_supply_backordered AS (
  SELECT
    TRUE AS is_under_supply_backordered_boolean
    , 'Undersupply Backordered' AS is_under_supply_backordered

  UNION ALL

  SELECT
    FALSE AS is_under_supply_backordered_boolean
    , 'Not Undersupply Backordered' AS is_under_supply_backordered

)

SELECT
  CONCAT(dim_is_under_supply_backordered.is_under_supply_backordered_boolean, ',' , dim_package_type.package_type_key) AS sales_order_line_indicator_key
  , dim_is_under_supply_backordered.is_under_supply_backordered_boolean
  , dim_is_under_supply_backordered.is_under_supply_backordered
  , dim_package_type.package_type_key
  , dim_package_type.package_type_name
FROM dim_is_under_supply_backordered
CROSS JOIN {{ ref('dim_package_type') }}