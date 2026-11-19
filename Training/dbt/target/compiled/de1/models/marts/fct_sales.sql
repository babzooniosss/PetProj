SELECT
    oi.order_item_id,
    o.order_id,
    o.order_date,
    o.order_date::date AS order_date_day,
    o.customer_id,
    c.first_name,
    c.last_name,
    c.city,
    oi.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS revenue,
    o.status
FROM "mydb"."mart"."stg_order_items" oi
JOIN "mydb"."mart"."stg_orders" o
    ON oi.order_id = o.order_id
JOIN "mydb"."mart"."stg_products" p
    ON oi.product_id = p.product_id
JOIN "mydb"."mart"."stg_customers" c
    ON o.customer_id = c.customer_id