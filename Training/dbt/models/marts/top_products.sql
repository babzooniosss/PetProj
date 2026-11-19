SELECT
    product_id,
    product_name,
    category,
    SUM(quantity) AS units_sold,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT order_id) AS orders_count
FROM {{ ref('fct_sales') }}
WHERE status = 'completed'
GROUP BY
    product_id,
    product_name,
    category
ORDER BY total_revenue DESC