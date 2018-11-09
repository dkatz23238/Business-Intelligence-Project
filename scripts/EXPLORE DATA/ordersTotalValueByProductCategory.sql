
/* Orders total value by product category  */
SELECT
    ROUND ( SUM(orders_general.order_products_value)) as total_value,
    orders_general.product_category_name,
    cat_names.product_category_name_english
FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY total_value DESC;
