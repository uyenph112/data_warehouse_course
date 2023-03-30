WITH dim_supplier__source AS(
  SELECT 
    *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

,dim_supplier__rename_column AS(
  SELECT
    supplier_id AS supplier_key
    , supplier_name AS supplier_name
    , payment_days
    , primary_contact_person_id AS primary_contact_person_key
    , alternate_contact_person_id AS alternate_contact_person_key
    , supplier_category_id AS supplier_category_key
    , delivery_method_id AS delivery_method_key
    , delivery_city_id AS delivery_city_key
    , postal_city_id AS postal_city_key
  FROM dim_supplier__source
)

,dim_supplier__cast_type AS(
  SELECT
    CAST(supplier_key AS int) AS supplier_key
    , CAST(supplier_name AS string) AS supplier_name
    , CAST(payment_days AS INT) AS payment_days
    , CAST(primary_contact_person_key AS INT) AS primary_contact_person_key
    , CAST(alternate_contact_person_key AS INT) AS alternate_contact_person_key
    , CAST(supplier_category_key AS INT) AS supplier_category_key
    , CAST(delivery_method_key AS INT) AS delivery_method_key
    , CAST(delivery_city_key AS INT) AS delivery_city_key
    , CAST(postal_city_key AS INT) AS postal_city_key
  FROM dim_supplier__rename_column
)

SELECT
  dim_supplier.supplier_key
  , dim_supplier.supplier_name
  , dim_supplier.payment_days
  , dim_supplier.primary_contact_person_key
  , COALESCE(dim_primary_person.person_name, 'Invalid') AS primary_contact_person_name
  , dim_supplier.alternate_contact_person_key
  , COALESCE(dim_alternate_person.person_name, 'Invalid') AS alternate_contact_person_name
  , dim_supplier.supplier_category_key
  , COALESCE(dim_supplier_category.supplier_category_name, 'Invalid') AS supplier_category_name
  , dim_supplier.delivery_method_key
  , COALESCE(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
  , dim_supplier.delivery_city_key
  , COALESCE(dim_delivery_city.city_name, 'Invalid') AS delivery_city_name 
  , COALESCE(dim_delivery_city.state_province_name, 'Invalid') AS delivery_state_province_name
  , COALESCE(dim_delivery_city.country_name, 'Invalid') AS delivery_country_name
  , dim_supplier.postal_city_key
  , COALESCE(dim_postal_city.city_name, 'Invalid') AS postal_city_name 
  , COALESCE(dim_postal_city.state_province_name, 'Invalid') AS postal_state_province_name
  , COALESCE(dim_postal_city.country_name, 'Invalid') AS postal_country_name
FROM dim_supplier__cast_type AS dim_supplier 
LEFT JOIN {{ ref('dim_person') }} AS dim_primary_person
  ON dim_supplier.primary_contact_person_key = dim_primary_person.person_key
LEFT JOIN {{ ref('dim_person') }} AS dim_alternate_person
  ON dim_supplier.alternate_contact_person_key = dim_alternate_person.person_key
LEFT JOIN {{ ref('stg_dim_supplier_category') }} AS dim_supplier_category
  ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key
LEFT JOIN {{ ref('stg_dim_delivery_method') }} AS dim_delivery_method
  ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_delivery_city 
  ON dim_supplier.delivery_city_key = dim_delivery_city.city_key
LEFT JOIN {{ ref('stg_dim_city') }} AS dim_postal_city
  ON dim_supplier.postal_city_key = dim_postal_city.city_key