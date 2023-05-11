SELECT
  product_category_key AS parent_category_key 
  , product_category_name AS parent_category_name
  , product_category_key AS child_category_key 
  , product_category_name AS child_category_name
  , 0 AS depth_from_parent
FROM {{ ref('stg_dim_external_product_category')}}

UNION ALL

SELECT
  parent_category_key
  , parent_category_name
  , product_category_key
  , product_category_name
  , 1 AS depth_from_parent
FROM {{ ref('stg_dim_external_product_category')}}
WHERE parent_category_key <> 0

UNION ALL

SELECT
  parent.parent_category_key 
  , parent.parent_category_name
  , child.product_category_key AS child_category_key
  , child.product_category_name AS child_category_name
  , 2 AS depth_from_parent
FROM {{ ref('stg_dim_external_product_category')}} AS child 
LEFT JOIN {{ ref('stg_dim_external_product_category')}} AS parent
  ON child.parent_category_key = parent.product_category_key
WHERE parent.parent_category_key <> 0

UNION ALL

SELECT
  g_parent.parent_category_key 
  , g_parent.parent_category_name
  , child.product_category_key AS child_category_key
  , child.product_category_name AS child_category_name
  , 3 AS depth_from_parent
FROM {{ ref('stg_dim_external_product_category')}} AS child 
LEFT JOIN {{ ref('stg_dim_external_product_category')}} AS parent
  ON child.parent_category_key = parent.product_category_key
LEFT JOIN {{ ref('stg_dim_external_product_category')}} AS g_parent
  ON parent.parent_category_key = g_parent.product_category_key
WHERE g_parent.parent_category_key <> 0