-- ========================
-- OVERVIEW / MARKETPLACE SUMMARY
-- ========================

-- Total Revenue
SELECT SUM(price + freight_value) AS total_revenue
FROM gold.fact_sales;

-- Total Orders
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM gold.fact_sales;

-- Average Order Value
SELECT AVG(order_total) AS average_order_value
FROM (
    SELECT order_id, SUM(price + freight_value) AS order_total
    FROM gold.fact_sales
    GROUP BY order_id
) t;

-- Monthly Marketplace Growth (Revenue Trend)
SELECT
    YEAR(order_purchase_timestamp) AS year,
    MONTH(order_purchase_timestamp) AS month,
    SUM(price + freight_value) AS revenue,
    COUNT(DISTINCT order_id) AS orders
FROM gold.fact_sales
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY year, month;


-- ========================
-- CATEGORY PERFORMANCE
-- ========================

-- Revenue by Category
SELECT
    COALESCE(p.product_category_name_english, 'uncategorized') AS product_category_name,
    SUM(f.price + f.freight_value) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY revenue DESC;

-- Top Categories by Orders
SELECT TOP 10
    COALESCE(p.product_category_name_english, 'uncategorized') AS product_category_name,
    COUNT(DISTINCT f.order_id) AS orders
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY orders DESC;

-- Category Growth (Monthly)
SELECT
    COALESCE(p.product_category_name_english, 'uncategorized') AS product_category_name,
    YEAR(f.order_purchase_timestamp) AS year,
    MONTH(f.order_purchase_timestamp) AS month,
    SUM(f.price + f.freight_value) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category_name_english, YEAR(f.order_purchase_timestamp), MONTH(f.order_purchase_timestamp)
ORDER BY year, month, revenue DESC;


-- ========================
-- SELLER PERFORMANCE
-- ========================

-- Seller Summary
SELECT
    s.seller_id,
    SUM(f.price + f.freight_value) AS revenue,
    COUNT(DISTINCT f.order_id) AS orders,
    AVG(f.review_score) AS avg_review_score
FROM gold.fact_sales f
JOIN gold.dim_seller s ON f.seller_id = s.seller_id
GROUP BY s.seller_id
ORDER BY revenue DESC;

-- Top 10 Sellers by Revenue
SELECT TOP 10
    seller_id,
    SUM(price + freight_value) AS revenue
FROM gold.fact_sales
GROUP BY seller_id
ORDER BY revenue DESC;

-- Seller Revenue Distribution Buckets
WITH seller_revenue_bucket AS (
    SELECT seller_id, SUM(price + freight_value) AS revenue
    FROM gold.fact_sales
    GROUP BY seller_id
),
bucketed AS (
    SELECT
        seller_id,
        CASE
            WHEN revenue < 1000 THEN 'Low Revenue'
            WHEN revenue BETWEEN 1000 AND 10000 THEN 'Medium Revenue'
            WHEN revenue BETWEEN 10001 AND 50000 THEN 'High Revenue'
            ELSE 'Top Sellers'
        END AS revenue_group
    FROM seller_revenue_bucket
)
SELECT
    revenue_group,
    COUNT(*) AS sellers
FROM bucketed
GROUP BY revenue_group
ORDER BY
    CASE
        WHEN revenue_group = 'Low Revenue' THEN 1
        WHEN revenue_group = 'Medium Revenue' THEN 2
        WHEN revenue_group = 'High Revenue' THEN 3
        ELSE 4
    END;

-- Seller Review Score Distribution
WITH seller_avg_reviews AS (
    SELECT 
        seller_id,
        AVG(review_score) AS avg_review_score
    FROM gold.fact_sales
    GROUP BY seller_id
)
SELECT
    CASE 
        WHEN avg_review_score < 2 THEN 'Poor'
        WHEN avg_review_score >= 2 AND avg_review_score < 3.5 THEN 'Average'
        WHEN avg_review_score >= 3.5 AND avg_review_score < 4.5 THEN 'Good'
        ELSE 'Excellent'
    END AS review_bucket,
    COUNT(*) AS sellers
FROM seller_avg_reviews
GROUP BY
    CASE 
        WHEN avg_review_score < 2 THEN 'Poor'
        WHEN avg_review_score >= 2 AND avg_review_score < 3.5 THEN 'Average'
        WHEN avg_review_score >= 3.5 AND avg_review_score < 4.5 THEN 'Good'
        ELSE 'Excellent'
    END
ORDER BY review_bucket;

-- Top 10% Sellers Contribution to Revenue
WITH seller_revenue_ranked AS (
    SELECT seller_id, SUM(price + freight_value) AS revenue,
           NTILE(10) OVER (ORDER BY SUM(price + freight_value) DESC) AS revenue_decile
    FROM gold.fact_sales
    GROUP BY seller_id
)
SELECT
    SUM(revenue) AS top_10_percent_revenue,
    SUM(revenue) * 100.0 / (SELECT SUM(SUM(price + freight_value)) FROM gold.fact_sales) AS top_10_percent_share
FROM seller_revenue_ranked
WHERE revenue_decile = 1;

-- Seller Delivery Performance
SELECT
    seller_id,
    AVG(delivery_days) AS avg_delivery_time,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN delivery_days > estimated_delivery_days THEN 1 ELSE 0 END) AS late_deliveries,
    CAST(SUM(CASE WHEN delivery_days > estimated_delivery_days THEN 1 ELSE 0 END) AS DECIMAL(5,2))
        * 100.0 / COUNT(*) AS late_delivery_rate
FROM gold.fact_sales
GROUP BY seller_id
ORDER BY late_delivery_rate DESC;

-- Seller Churn Risk
WITH monthly_orders AS (
    SELECT
        seller_id,
        YEAR(order_purchase_timestamp) AS year,
        MONTH(order_purchase_timestamp) AS month,
        COUNT(DISTINCT order_id) AS orders
    FROM gold.fact_sales
    GROUP BY seller_id, YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
),
order_trend AS (
    SELECT seller_id, orders,
           LAG(orders) OVER (PARTITION BY seller_id ORDER BY year, month) AS prev_month_orders
    FROM monthly_orders
)
SELECT
    seller_id,
    COUNT(*) AS months_declining,
    SUM(CASE WHEN orders < prev_month_orders THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS declining_rate_percentage
FROM order_trend
WHERE prev_month_orders IS NOT NULL
GROUP BY seller_id
HAVING SUM(CASE WHEN orders < prev_month_orders THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 50
ORDER BY declining_rate_percentage DESC;


-- ========================
-- CUSTOMER EXPERIENCE
-- ========================

-- Average Review Score & Delivery Time
SELECT
    AVG(delivery_days) AS avg_delivery_time,
    AVG(review_score) AS avg_review_score
FROM gold.fact_sales;

-- Late Deliveries
SELECT COUNT(*) AS late_deliveries
FROM gold.fact_sales
WHERE delivery_days > estimated_delivery_days;

-- Late Delivery Rate
SELECT CAST(COUNT(CASE WHEN delivery_days > estimated_delivery_days THEN 1 END) AS DECIMAL(5,2)) * 100.0
       / COUNT(*) AS late_delivery_rate
FROM gold.fact_sales;

-- Review Score Distribution
SELECT review_score, COUNT(*) AS total_reviews
FROM gold.fact_sales
GROUP BY review_score
ORDER BY review_score;

-- Customer Acquisition Trend (New Customers Over Time)
SELECT
    YEAR(order_purchase_timestamp) AS year,
    MONTH(order_purchase_timestamp) AS month,
    COUNT(DISTINCT customer_id) AS new_customers
FROM gold.fact_sales
GROUP BY YEAR(order_purchase_timestamp), MONTH(order_purchase_timestamp)
ORDER BY year, month;

-- Customer Spending Distribution
SELECT
    customer_id,
    SUM(price + freight_value) AS total_spent
FROM gold.fact_sales
GROUP BY customer_id
ORDER BY total_spent DESC;


-- ========================
-- FREIGHT & REVENUE CONCENTRATION
-- ========================

-- Freight Analysis
SELECT
    AVG(freight_value) AS avg_freight,
    SUM(freight_value) AS total_freight
FROM gold.fact_sales;

-- Revenue Concentration (Cumulative Share)
WITH seller_revenue_dist AS (
    SELECT seller_id, SUM(price + freight_value) AS revenue
    FROM gold.fact_sales
    GROUP BY seller_id
)
SELECT
    seller_id,
    revenue,
    SUM(revenue) OVER (ORDER BY revenue DESC ROWS UNBOUNDED PRECEDING) 
        * 100.0 / SUM(revenue) OVER () AS cumulative_share
FROM seller_revenue_dist
ORDER BY revenue DESC;