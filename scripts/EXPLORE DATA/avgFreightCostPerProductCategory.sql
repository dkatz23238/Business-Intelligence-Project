/* Avg freight cost by category  */
SELECT
    ROUND( AVG(orders_general.order_freight_value))  freight_cost,
    orders_general.product_category_name,
    cat_names.product_category_name_english

FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY freight_cost DESC
