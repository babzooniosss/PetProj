
  create view "mydb"."mart"."stg_orders__dbt_tmp"
    
    
  as (
    SELECT
    order_id,
    customer_id,
    order_date,
    status
FROM "mydb"."raw"."orders"
  );