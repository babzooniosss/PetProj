SELECT
    category,
    SUM(quantity) AS units_sold,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT order_id) AS orders_count
FROM "mydb"."mart"."fct_sales"
WHERE status = 'completed'
GROUP BY category
ORDER BY total_revenue DESC