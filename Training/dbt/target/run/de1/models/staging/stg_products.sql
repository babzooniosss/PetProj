
  create view "mydb"."mart"."stg_products__dbt_tmp"
    
    
  as (
    SELECT
    product_id,
    product_name,
    category,
    price,
    created_at
FROM "mydb"."raw"."products"
  );