/* Products Value by Month - Year */
SELECT orders_general.month, orders_general.year, ROUND ( SUM ( order_products_value ) ) as products_value
FROM orders_general
GROUP BY year, month
ORDER by year, month;
