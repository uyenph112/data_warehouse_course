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
  supplier_key
  , supplier_name
FROM dim_supplier__cast_type AS dim_supplier 
LEFT JOIN 