SELECT SUM(price + freight_value)
FROM gold.fact_sales

SELECT SUM(price + freight_value)
FROM silver.olist_order_items

SELECT COUNT(*)
FROM gold.fact_sales

SELECT COUNT(*)
FROM silver.olist_order_items

SELECT COUNT(*)
FROM gold.fact_sales f
LEFT JOIN gold.dim_product p
ON f.product_id = p.product_id
WHERE p.product_id IS NULL