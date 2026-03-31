/*
================================================================================
Project: Olist Data Quality Checks
Purpose: This SQL script performs comprehensive data quality checks on the 
         'olist_customers' and 'olist_geolocation' tables from the bronze layer.
         The checks include:
         - Null checks
         - Duplicate identification
         - Unwanted spaces
         - Format and length validations
         - Geolocation and coordinate consistency
         - Cross-table validation

         This script is intended for data validation and cleansing preparation
         before downstream analytics or data warehouse transformations.
================================================================================
*/

-- =====================================================
-- 1. Sample Data Preview
-- =====================================================
SELECT TOP 5 *
FROM bronze.olist_customers;

SELECT TOP 10 *
FROM bronze.olist_geolocation;

SELECT TOP 10 *
FROM bronze.olist_order_items;

SELECT *
FROM bronze.olist_order_payments;

SELECT TOP 10 * 
FROM bronze.olist_order_reviews

SELECT TOP 10 *
FROM bronze.olist_orders

SELECT TOP 10 * 
FROM bronze.olist_products

SELECT TOP 10 *
FROM bronze.olist_sellers

SELECT TOP 10 *
FROM bronze.products_category_name_translation

-- =====================================================
-- 2. Customer Table: Null and Duplicate Checks
-- =====================================================

-- 2.1 Check for duplicate or NULL customer_id
SELECT
    customer_id,
    COUNT(*) AS record_count
FROM bronze.olist_customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;

-- 2.2 Check for unwanted spaces in customer_id
SELECT customer_id
FROM bronze.olist_customers
WHERE customer_id <> TRIM(customer_id);

-- 2.3 Check for duplicate or NULL customer_unique_id
SELECT
    customer_unique_id,
    COUNT(*) AS record_count
FROM bronze.olist_customers
GROUP BY customer_unique_id
HAVING COUNT(*) > 1 OR customer_unique_id IS NULL;

-- 2.4 Check for unwanted spaces in customer_unique_id
SELECT customer_unique_id
FROM bronze.olist_customers
WHERE customer_unique_id <> TRIM(customer_unique_id);

-- =====================================================
-- 3. Customer ZIP Code Checks
-- =====================================================

-- 3.1 Check length of ZIP code
SELECT DISTINCT LEN(customer_zip_code_prefix) AS zip_length
FROM bronze.olist_customers;

-- 3.2 Identify ZIP codes not 5 digits
SELECT customer_zip_code_prefix
FROM bronze.olist_customers
WHERE LEN(customer_zip_code_prefix) <> 5;

-- 3.3 Check for NULL ZIP codes
SELECT *
FROM bronze.olist_customers
WHERE customer_zip_code_prefix IS NULL;

-- =====================================================
-- 4. Customer City Checks
-- =====================================================

-- 4.1 Unwanted leading/trailing spaces
SELECT customer_city
FROM bronze.olist_customers
WHERE customer_city <> TRIM(customer_city);

-- 4.2 Empty string cities
SELECT *
FROM bronze.olist_customers
WHERE LTRIM(RTRIM(customer_city)) = '';

-- 4.3 Count of records per city
SELECT customer_city, COUNT(*)
FROM bronze.olist_customers
GROUP BY customer_city
ORDER BY customer_city;

-- 4.4 Cities with special characters
SELECT DISTINCT customer_city
FROM bronze.olist_customers
WHERE customer_city LIKE '%[^a-zA-Z ]%';

-- 4.5 Variations of the same city (case-insensitive)
SELECT 
    LOWER(LTRIM(RTRIM(customer_city))) AS normalized_city,
    COUNT(DISTINCT customer_city) AS variations,
    COUNT(*) AS total_rows
FROM bronze.olist_customers
GROUP BY LOWER(LTRIM(RTRIM(customer_city)))
HAVING COUNT(DISTINCT customer_city) > 1;

-- 4.6 Variations after replacing hyphens with spaces
SELECT 
    LOWER(REPLACE(LTRIM(RTRIM(customer_city)), '-', ' ')) AS normalized_city,
    COUNT(DISTINCT customer_city) AS variations
FROM bronze.olist_customers
GROUP BY LOWER(REPLACE(LTRIM(RTRIM(customer_city)), '-', ' '))
HAVING COUNT(DISTINCT customer_city) > 1;

-- =====================================================
-- 5. Customer State Checks
-- =====================================================

-- 5.1 Null or empty state
SELECT *
FROM bronze.olist_customers
WHERE customer_state IS NULL
   OR LTRIM(RTRIM(customer_state)) = '';

-- 5.2 Unwanted spaces in state
SELECT *
FROM bronze.olist_customers
WHERE customer_state <> LTRIM(RTRIM(customer_state));

-- 5.3 Distinct states
SELECT DISTINCT customer_state
FROM bronze.olist_customers;

-- 5.4 States not in valid Brazilian state codes
SELECT DISTINCT customer_state
FROM bronze.olist_customers
WHERE customer_state NOT IN (
    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
    'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
);

-- =====================================================
-- 6. Geolocation Table Checks
-- =====================================================

-- 6.1 Duplicate or NULL ZIP codes in geolocation
SELECT 
    geolocation_zip_code_prefix,
    COUNT(*) 
FROM bronze.olist_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1 OR geolocation_zip_code_prefix IS NULL;

-- 6.2 Distinct geolocation ZIP codes
SELECT DISTINCT geolocation_zip_code_prefix
FROM bronze.olist_geolocation;

-- 6.3 Validate latitude and longitude ranges
SELECT geolocation_lat, geolocation_lng
FROM bronze.olist_geolocation
WHERE TRY_CAST(geolocation_lat AS DECIMAL(10,8)) NOT BETWEEN -90 AND 90
  OR TRY_CAST(geolocation_lng AS DECIMAL(11,8)) NOT BETWEEN -180 AND 180;

-- 6.4 Unwanted spaces in city names
SELECT geolocation_city
FROM bronze.olist_geolocation
WHERE geolocation_city <> TRIM(geolocation_city);

-- 6.5 Empty string cities
SELECT *
FROM bronze.olist_geolocation
WHERE LTRIM(RTRIM(geolocation_city)) = '';

-- 6.6 Count records per city
SELECT geolocation_city, COUNT(*)
FROM bronze.olist_geolocation
GROUP BY geolocation_city
ORDER BY geolocation_city;

-- 6.7 Cities with special characters
SELECT DISTINCT geolocation_city
FROM bronze.olist_geolocation
WHERE geolocation_city LIKE '%[^a-zA-Z ]%';

-- 6.8 Variations of the same city (case-insensitive)
SELECT 
    LOWER(LTRIM(RTRIM(geolocation_city))) AS normalized_city,
    COUNT(DISTINCT geolocation_city) AS variations,
    COUNT(*) AS total_rows
FROM bronze.olist_geolocation
GROUP BY LOWER(LTRIM(RTRIM(geolocation_city)))
HAVING COUNT(DISTINCT geolocation_city) > 1;

-- 6.9 Variations after replacing hyphens with spaces
SELECT 
    LOWER(REPLACE(LTRIM(RTRIM(geolocation_city)), '-', ' ')) AS normalized_city,
    COUNT(DISTINCT geolocation_city) AS variations
FROM bronze.olist_geolocation
GROUP BY LOWER(REPLACE(LTRIM(RTRIM(geolocation_city)), '-', ' '))
HAVING COUNT(DISTINCT geolocation_city) > 1;

-- 6.10 Coordinate spread validation (check for outliers)
SELECT
    geolocation_zip_code_prefix,
    MIN(geolocation_lat) AS min_lat,
    MAX(geolocation_lat) AS max_lat,
    MIN(geolocation_lng) AS min_lng,
    MAX(geolocation_lng) AS max_lng
FROM bronze.olist_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING
    TRY_CAST(MAX(geolocation_lat) AS DECIMAL(18,16)) - TRY_CAST(MIN(geolocation_lat) AS DECIMAL(18,16)) > 1
    OR
    TRY_CAST(MAX(geolocation_lng) AS DECIMAL(18,16)) - TRY_CAST(MIN(geolocation_lng) AS DECIMAL(18,16)) > 1;

-- 6.11 State consistency validation per ZIP code
SELECT
    geolocation_zip_code_prefix,
    COUNT(DISTINCT geolocation_state) AS state_count
FROM bronze.olist_geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_state) > 1;

-- =====================================================
-- 7. Cross-Table Validation
-- =====================================================

-- 7.1 Customer ZIP coverage in geolocation table
SELECT DISTINCT c.customer_zip_code_prefix
FROM bronze.olist_customers c
LEFT JOIN bronze.olist_geolocation g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL;

-- =====================================================
-- 8. Order Items Table Checks
-- =====================================================

-- 8.1 Mandatory Columns / Null Checks
SELECT *
FROM bronze.olist_order_items
WHERE order_id IS NULL
   OR order_item_id IS NULL
   OR product_id IS NULL
   OR seller_id IS NULL;

-- 8.2 Data Type Validations 
SELECT *
FROM bronze.olist_order_items
WHERE TRY_CAST(price AS DECIMAL(10,2)) IS NULL
   OR TRY_CAST(freight_value AS DECIMAL(10,2)) IS NULL;

-- 8.3 Range Checks / Logical Validation
SELECT *
FROM bronze.olist_order_items
WHERE TRY_CAST(price AS DECIMAL(10,3)) < 0 
      OR TRY_CAST(freight_value AS DECIMAL(10,3)) < 0 
      OR TRY_CAST(order_item_id AS INT) <= 0;

-- 8.4 Duplicate Check
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, order_item_id
               ORDER BY _ingestion_datetime DESC
           ) AS rn
    FROM bronze.olist_order_items
) t
WHERE rn = 2


-- 8.5 Date / Timestamp Validation
SELECT *
FROM bronze.olist_order_items
WHERE TRY_CAST(shipping_limit_date AS DATETIME) IS NULL
   OR shipping_limit_date > SYSUTCDATETIME();

-- 8.6 Cross-Table Referential Integrity
SELECT oi.*
FROM bronze.olist_order_items oi
LEFT JOIN bronze.olist_orders o
  ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 8.7 Negative / Zero Freight vs Price Ratios
SELECT *
FROM bronze.olist_order_items
WHERE TRY_CAST(freight_value AS DECIMAL(10,3)) < 0
   OR TRY_CAST(price AS DECIMAL(10,3)) < 0
   OR TRY_CAST(freight_value AS DECIMAL(10,3)) > TRY_CAST(price AS DECIMAL(10,3)) * 10; -- arbitrary rule for outliers

-- 8.8 Standardization / Cleaning
SELECT 
    *
FROM bronze.olist_order_items
WHERE order_id <> TRIM(order_id) 
    OR order_item_id <> TRIM(order_item_id)
    OR product_id <> TRIM(product_id)
    OR seller_id <> TRIM(seller_id)
    OR shipping_limit_date <> TRIM(shipping_limit_date)
    OR price <> TRIM(price)
    OR freight_value <> TRIM(freight_value)


-- 8.9 Source / Load Metadata

-- 8.10 Row Count / Monitoring

-- 8.11 Order item sequencing: check order_item_id starts at 1 per order

-- 8.12 High-value orders: flag orders where price or freight_value are extreme outliers

-- 8.13 Consistency check: sum of price per order in order_items vs orders.total_amount

-- =====================================================
-- 9. Order Payments Table Checks
-- =====================================================

-- 9.1 Mandatory Column / Null Checks
SELECT *
FROM bronze.olist_order_payments
WHERE order_id IS NULL
   OR payment_sequential IS NULL
   OR payment_type IS NULL
   OR payment_installments IS NULL
   OR payment_value IS NULL;

-- 9.2 Data Type Validation
SELECT *
FROM bronze.olist_order_payments
WHERE TRY_CAST(payment_sequential AS INT) IS NULL
   OR TRY_CAST(payment_installments AS INT) IS NULL
   OR TRY_CAST(payment_value AS DECIMAL(10,2)) IS NULL;

-- 9.3 Range Checks / Logical Validation
SELECT *
FROM bronze.olist_order_payments
WHERE TRY_CAST(payment_sequential AS INT) < 1
   OR TRY_CAST(payment_installments AS INT) < 0
   OR TRY_CAST(payment_value AS DECIMAL(10,2)) < 0;

-- 9.4 Deduplication
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, payment_sequential
               ORDER BY _ingestion_datetime DESC
           ) AS rn
    FROM bronze.olist_order_payments
)
SELECT *
FROM cte
WHERE rn = 2;

-- 9.5 Categorical Validation (payment_type)
SELECT DISTINCT
    payment_type
FROM bronze.olist_order_payments

-- 9.6 Sequential Validation
SELECT order_id, COUNT(*) AS count_seq, MAX(payment_sequential) AS max_seq
FROM silver.olist_order_payments
GROUP BY order_id
HAVING count(*) <> MAX(payment_sequential);

-- 9.7 Cross-Table Referential Integrity
SELECT p.*
FROM bronze.olist_order_payments p
LEFT JOIN bronze.olist_orders o
  ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 9.8 Total Payment vs Order Value

-- =====================================================
-- 10. Order Review Table Checks
-- =====================================================

-- 10.1 Mandatory Columns / Null Checks
SELECT *
FROM bronze.olist_order_reviews
WHERE review_id IS NULL
   OR order_id IS NULL
   OR review_score IS NULL
   OR review_creation_date IS NULL;

-- 10.2 Data Type Validation
SELECT *
FROM bronze.olist_order_reviews
WHERE TRY_CAST(review_score AS INT) IS NULL
   OR TRY_CAST(review_creation_date AS DATETIME) IS NULL
   OR (review_answer_timestamp IS NOT NULL AND TRY_CAST(review_answer_timestamp AS DATETIME) IS NULL);

-- 10.3 Range Checks / Logical Validation
SELECT *
FROM bronze.olist_order_reviews
WHERE review_score NOT BETWEEN 1 AND 5
   OR (review_answer_timestamp IS NOT NULL AND review_answer_timestamp < review_creation_date);

-- 10.4 Deduplication
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY review_id
               ORDER BY _ingestion_datetime DESC
           ) AS rn
    FROM bronze.olist_order_reviews
)
SELECT *
FROM cte
WHERE rn > 1;

-- 10.5 Text Normalization (Titles and Messages)


-- 10.6 Referential Integrity
SELECT r.*
FROM bronze.olist_order_reviews r
LEFT JOIN bronze.olist_orders o
  ON r.order_id = o.order_id
WHERE o.order_id IS NULL;

-- 10.7 Timestamp Validation
SELECT *
FROM bronze.olist_order_reviews
WHERE review_creation_date > SYSUTCDATETIME()
   OR (review_answer_timestamp IS NOT NULL AND review_answer_timestamp > SYSUTCDATETIME());

-- 10.8 


-- =====================================================
-- 11. Orders Table Checks
-- =====================================================

-- 11.1 Mandatory Column Checks
SELECT *
FROM bronze.olist_orders
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR order_status IS NULL
   OR order_purchase_timestamp IS NULL;

-- 11.2 Data Type Validation
SELECT *
FROM bronze.olist_orders
WHERE TRY_CAST(order_purchase_timestamp AS DATETIME2) IS NULL
   OR (order_approved_at IS NOT NULL 
       AND TRY_CAST(order_approved_at AS DATETIME2) IS NULL)
   OR (order_delivered_carrier_date IS NOT NULL 
       AND TRY_CAST(order_delivered_carrier_date AS DATETIME2) IS NULL)
   OR (order_delivered_customer_date IS NOT NULL 
       AND TRY_CAST(order_delivered_customer_date AS DATETIME2) IS NULL)
   OR TRY_CAST(order_estimated_delivery_date AS DATETIME2) IS NULL;

-- 11.3 Order Status Validation
SELECT DISTINCT order_status
FROM bronze.olist_orders
WHERE order_status NOT IN (
'created','approved','processing','shipped',
'delivered','canceled','unavailable','invoiced'
);

-- 11.4 Timestamp Logical Order Checks
SELECT *
FROM bronze.olist_orders
WHERE
    order_approved_at >= order_purchase_timestamp
 OR order_delivered_carrier_date >= order_approved_at
 OR order_delivered_customer_date >= order_delivered_carrier_date;

-- 11.5 Estimated Delivery Validation
SELECT *
FROM bronze.olist_orders
WHERE order_estimated_delivery_date < order_purchase_timestamp;

-- 11.6 Future Timestamp Validation
SELECT *
FROM bronze.olist_orders
WHERE order_purchase_timestamp > SYSUTCDATETIME()
   OR order_approved_at > SYSUTCDATETIME()
   OR order_delivered_carrier_date > SYSUTCDATETIME()
   OR order_delivered_customer_date > SYSUTCDATETIME();

-- 11.7 Deduplication
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY _ingestion_datetime DESC
           ) rn
    FROM bronze.olist_orders
)
SELECT *
FROM cte
WHERE rn > 1;

-- 11.8 Referential Integrity
SELECT o.*
FROM bronze.olist_orders o
LEFT JOIN bronze.olist_customers c
ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 11.9 
SELECT *
FROM bronze.olist_orders
WHERE order_status = 'delivered'
AND order_delivered_customer_date IS NULL;


-- =====================================================
-- 12. Products Table Checks
-- =====================================================

-- 12.1 
SELECT *
FROM bronze.olist_products
WHERE product_id IS NULL
   OR product_category_id IS NULL
   OR product_name_lenght IS NULL
   OR product_description_lenght IS NULL;

SELECT COUNT(*)
FROM bronze.olist_order_items oi
LEFT JOIN bronze.olist_products p
ON oi.product_id = p.product_id
WHERE p.product_id IS NULL

-- 12.2 
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY product_id
               ORDER BY _ingestion_datetime DESC
           ) AS rn
    FROM bronze.olist_products
)
SELECT *
FROM cte
WHERE rn > 1;

-- 12.3
SELECT *
FROM bronze.olist_products
WHERE TRY_CAST(product_weight_g AS DECIMAL(10,3)) <= 0
   OR TRY_CAST(product_length_cm AS DECIMAL(10,3)) <= 0
   OR TRY_CAST(product_height_cm AS DECIMAL(10,3)) <= 0
   OR TRY_CAST(product_width_cm AS DECIMAL(10,3)) <= 0
   OR TRY_CAST(product_photos_qty AS INT) < 0
   OR TRY_CAST(product_name_lenght AS INT) <= 0
   OR TRY_CAST(product_description_lenght AS INT) < 0;

-- =====================================================
-- 13. Sellers Table Checks
-- =====================================================

-- 13.1 


-- =====================================================
-- 14. Product Category Name Translation Table Checks
-- =====================================================

-- 14.1
SELECT *
FROM bronze.products_category_name_translation
WHERE product_category_name IS NULL
   OR product_category_name_english IS NULL
   OR LTRIM(RTRIM(product_category_name)) = ''
   OR LTRIM(RTRIM(product_category_name_english)) = '';


SELECT product_category_name, COUNT(*) AS cnt
FROM bronze.products_category_name_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1;

--
SELECT p.product_category_id
FROM bronze.olist_products p
LEFT JOIN silver.products_category_name_translation t
    ON LOWER(TRIM(p.product_category_id)) = t.product_category_name
WHERE t.product_category_name IS NULL;



SELECT *
FROM bronze.olist_order_items
WHERE TRY_CAST(order_item_id AS INT) IS NULL
  AND order_item_id IS NOT NULL;

SELECT *
FROM bronze.olist_order_payments
WHERE TRY_CAST(payment_sequential AS INT) IS NULL
   OR TRY_CAST(payment_installments AS INT) IS NULL;

   SELECT *
FROM bronze.olist_geolocation
WHERE TRY_CAST(geolocation_lat AS INT) IS NULL
   OR TRY_CAST(geolocation_lng AS INT) IS NULL;

SELECT * FROM bronze.olist_geolocation