SELECT
    order_date_day,
    SUM(revenue) AS daily_revenue,
    COUNT(DISTINCT order_id) AS orders_count
FROM "mydb"."mart"."fct_sales"
WHERE status = 'completed'
GROUP BY order_date_day
ORDER BY order_date_day