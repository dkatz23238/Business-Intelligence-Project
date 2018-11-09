/* Products Value by Date */
SELECT CAST(order_purchase_timestamp AS DATE) as dt , SUM ( order_products_value ) FROM orders_general
GROUP BY dt 
ORDER by dt ;

/* Products Value by Month - Year */
SELECT orders_general.month, orders_general.year, ROUND ( SUM ( order_products_value ) ) as products_value
FROM orders_general
GROUP BY year, month
ORDER by year, month;

/* Orders by product category  */
SELECT
    COUNT(orders_general.order_id) as orders,
    orders_general.product_category_name,
    cat_names.product_category_name_english
FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY orders DESC;


/* Orders total value by product category  */
SELECT
    ROUND ( SUM(orders_general.order_products_value)) as total_value,
    orders_general.product_category_name,
    cat_names.product_category_name_english
FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY total_value DESC;

/* Avg product value by category  */
SELECT
    ROUND ( AVG (orders_general.order_products_value)) as total_value,
    orders_general.product_category_name,
    cat_names.product_category_name_english

FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY total_value DESC

/* Avg freight cost by category  */
SELECT
    ROUND( AVG(orders_general.order_freight_value))  freight_cost,
    orders_general.product_category_name,
    cat_names.product_category_name_english

FROM orders_general
    LEFT JOIN cat_names ON cat_names.product_category_name = orders_general.product_category_name
GROUP BY cat_names.product_category_name_english, orders_general.product_category_name
ORDER BY freight_cost DESC

/* Orders by Status  */
SELECT
    COUNT(orders_general.order_id) as orders,
    orders_general.order_status as order_status
FROM orders_general
GROUP BY order_status
ORDER BY orders;


