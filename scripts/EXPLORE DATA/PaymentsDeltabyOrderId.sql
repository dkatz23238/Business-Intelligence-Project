/* Payments Delta */
SELECT 
payments.order_id as p_order_id, 
orders_general.order_id as general_order_id, 
SUM (payments.value ) as payments_value, 
SUM( orders_general.order_freight_value +  
orders_general.order_products_value) as orders_value
FROM payments
LEFT JOIN orders_general
ON payments.order_id = orders_general.order_id
GROUP BY payments.order_id , orders_general.order_id
