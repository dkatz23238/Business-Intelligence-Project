/* Orders by Status  */
SELECT
    COUNT(orders_general.order_id) as orders,
    orders_general.order_status as order_status
FROM orders_general
GROUP BY order_status
ORDER BY orders;

