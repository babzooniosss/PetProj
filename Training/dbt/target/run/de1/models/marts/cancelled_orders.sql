
  
    

  create  table "mydb"."mart"."cancelled_orders__dbt_tmp"
  
  
    as
  
  (
    SELECT
    order_id,
    order_date,
    order_date_day,
    customer_id,
    city,
    SUM(revenue) AS cancelled_revenue,
    COUNT(*) AS items_count
FROM "mydb"."mart"."fct_sales"
WHERE status = 'cancelled'
GROUP BY
    order_id,
    order_date,
    order_date_day,
    customer_id,
    city
ORDER BY order_date DESC
  );
  