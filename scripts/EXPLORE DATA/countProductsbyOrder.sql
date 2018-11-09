SELECT 
orders_general.order_id as order_id, 
COUNT(orders_general.product_id) as products_purchased

FROM orders_general
GROUP BY order_id
ORDER BY products_purchased ASC