CREATE OR ALTER VIEW gold.dim_product AS
SELECT 
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM silver.olist_products p
LEFT JOIN silver.products_category_name_translation t
ON p.product_category_name = t.product_category_name;
GO


CREATE OR ALTER VIEW gold.dim_customers AS 
SELECT 
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM silver.olist_customers;
GO


CREATE OR ALTER VIEW gold.dim_seller AS
SELECT
    seller_id,
    seller_city,
    seller_state
FROM silver.olist_sellers;
GO


CREATE OR ALTER VIEW gold.dim_date AS
SELECT DISTINCT
    CAST(order_purchase_timestamp AS DATE) AS date,
    DAY(order_purchase_timestamp) AS day,
    MONTH(order_purchase_timestamp) AS month,
    YEAR(order_purchase_timestamp) AS year,
    DATEPART(QUARTER, order_purchase_timestamp) AS quarter,
    DATENAME(WEEKDAY, order_purchase_timestamp) AS day_of_week
FROM silver.olist_orders;
GO


CREATE OR ALTER VIEW gold.dim_payment_type AS
SELECT
    payment_type,
    payment_installments
FROM silver.olist_order_payments;
GO


CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    o.order_purchase_timestamp,
    oi.price,
    oi.freight_value,
    ore.review_score,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_days,
    DATEDIFF(DAY, o.order_purchase_timestamp, o.order_estimated_delivery_date) AS estimated_delivery_days,
    op.total_payment
FROM silver.olist_order_items oi
LEFT JOIN silver.olist_orders o
    ON oi.order_id = o.order_id
LEFT JOIN (
    SELECT
        order_id,
        AVG(review_score) AS review_score
    FROM silver.olist_order_reviews
    GROUP BY order_id
) ore
ON ore.order_id = o.order_id
LEFT JOIN (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM silver.olist_order_payments
    GROUP BY order_id
) op
    ON op.order_id = o.order_id;
GO