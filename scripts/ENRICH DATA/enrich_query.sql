SELECT
orders_general.order_id,
orders_general.order_products_value,
orders_general.order_freight_value,
orders_general.order_delivered_customer_date,
orders_general.product_id,
cat_names.product_category_name_english,
orders_distance.order_distance,
orders_general.month,
orders_general.year,
products.product_height_cm,
products.product_length_cm ,
products.product_weight_g,
products.product_width_cm

FROM
orders_general
    JOIN orders_distance
        ON orders_distance.order_id = orders_general.order_id

     JOIN products
        ON orders_general.product_id = products.product_id  

    JOIN cat_names
        ON orders_general.product_category_name = cat_names.product_category_name_english    
WHERE orders_general.order_delivered_customer_date IS NOT NULL
ORDER BY orders_general.year, orders_general.month
