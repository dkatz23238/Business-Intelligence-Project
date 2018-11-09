/* Products Value by Date */
SELECT CAST(order_purchase_timestamp AS DATE) as dt , SUM ( order_products_value )
FROM orders_general
GROUP BY dt
ORDER by dt ;
