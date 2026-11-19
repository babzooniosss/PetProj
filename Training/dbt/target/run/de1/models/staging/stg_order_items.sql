
  create view "mydb"."mart"."stg_order_items__dbt_tmp"
    
    
  as (
    SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
FROM "mydb"."raw"."order_items"
  );