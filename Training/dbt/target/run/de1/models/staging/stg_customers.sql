
  create view "mydb"."mart"."stg_customers__dbt_tmp"
    
    
  as (
    SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    created_at
FROM "mydb"."raw"."customers"
  );