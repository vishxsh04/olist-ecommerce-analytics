CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    -- ------------------------------
    -- Load Customers
    -- ------------------------------
    TRUNCATE TABLE silver.olist_customers;

    INSERT INTO silver.olist_customers (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _ingestion_datetime
    )
    SELECT 
        customer_id,
        customer_unique_id,
        RIGHT('00000' + ISNULL(LTRIM(RTRIM(customer_zip_code_prefix)), ''), 5),
        LOWER(REPLACE(LTRIM(RTRIM(customer_city)),'-', ' ')),
        UPPER(LTRIM(RTRIM(customer_state))),
        SYSUTCDATETIME()
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id
                ORDER BY _ingestion_datetime DESC
            ) AS rn
        FROM bronze.olist_customers
        WHERE customer_id IS NOT NULL
    ) t
    WHERE rn = 1;
    
    -- ------------------------------
    -- Load Geolocation
    -- ------------------------------
    TRUNCATE TABLE silver.olist_geolocation;

    INSERT INTO silver.olist_geolocation (
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        _ingestion_datetime
    )
    SELECT
        RIGHT('00000' + ISNULL(LTRIM(RTRIM(geolocation_zip_code_prefix)), ''), 5) AS zip_prefix,
        AVG(CAST(geolocation_lat AS DECIMAL(18,16))) AS geolocation_lat,
        AVG(CAST(geolocation_lng AS DECIMAL(18,16))) AS geolocation_lng,
        LOWER(REPLACE(LTRIM(RTRIM(geolocation_city)) COLLATE Latin1_General_CI_AI, '-', ' ')) AS city,
        UPPER(LTRIM(RTRIM(geolocation_state))) AS state,
        SYSUTCDATETIME() AS _ingestion_datetime
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    RIGHT('00000' + ISNULL(LTRIM(RTRIM(geolocation_zip_code_prefix)), ''), 5),
                    LOWER(REPLACE(LTRIM(RTRIM(geolocation_city)) COLLATE Latin1_General_CI_AI, '-', ' ')),
                    UPPER(LTRIM(RTRIM(geolocation_state)))
                ORDER BY _ingestion_datetime DESC
            ) AS rn
        FROM bronze.olist_geolocation
        WHERE
            geolocation_zip_code_prefix IS NOT NULL
            AND TRY_CAST(geolocation_lat AS DECIMAL(18,16)) BETWEEN -90 AND 90
            AND TRY_CAST(geolocation_lng AS DECIMAL(18,16)) BETWEEN -180 AND 180
            AND LEN(LTRIM(RTRIM(geolocation_state))) = 2
    ) t
    WHERE rn = 1
    GROUP BY 
        RIGHT('00000' + ISNULL(LTRIM(RTRIM(geolocation_zip_code_prefix)), ''), 5),
        LOWER(REPLACE(LTRIM(RTRIM(geolocation_city)) COLLATE Latin1_General_CI_AI, '-', ' ')),
        UPPER(LTRIM(RTRIM(geolocation_state)));
    
    -- ------------------------------
    -- Load order items
    -- ------------------------------

    TRUNCATE TABLE silver.olist_order_items;

    INSERT INTO silver.olist_order_items (
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        _ingestion_datetime
    )
    SELECT 
        LOWER(LTRIM(RTRIM(order_id))),
        CAST(order_item_id AS INT),
        LOWER(LTRIM(RTRIM(product_id))),
        LOWER(LTRIM(RTRIM(seller_id))),
        CAST(shipping_limit_date AS DATETIME2),
        CAST(price AS DECIMAL(10,3)),
        CAST(freight_value AS DECIMAL(10,3)),
        SYSUTCDATETIME()
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, order_item_id
                ORDER BY _ingestion_datetime DESC
            ) AS rn
        FROM bronze.olist_order_items
        WHERE order_id IS NOT NULL AND order_item_id IS NOT NULL
    ) t
    WHERE rn = 1;

    -- ------------------------------
    -- Load order payments
    -- ------------------------------

    TRUNCATE TABLE silver.olist_order_payments;

    INSERT INTO silver.olist_order_payments (
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _ingestion_datetime
    )
    SELECT 
        LOWER(LTRIM(RTRIM(order_id))),
        CAST(LOWER(LTRIM(RTRIM(payment_sequential))) AS INT),
        LOWER(LTRIM(RTRIM(payment_type))),
        CAST(LTRIM(RTRIM(payment_installments)) AS INT),
        CAST(payment_value AS DECIMAL(10,3)),
        SYSUTCDATETIME()
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, payment_sequential
                ORDER BY _ingestion_datetime, payment_sequential DESC
            ) AS rn
        FROM bronze.olist_order_payments
        WHERE order_id IS NOT NULL
    )t
    WHERE rn = 1;

    
    -- ------------------------------
    -- Load order reviews
    -- ------------------------------

    TRUNCATE TABLE silver.olist_order_reviews;

    INSERT INTO silver.olist_order_reviews (
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _ingestion_datetime
    )
    SELECT
        LOWER(LTRIM(RTRIM(review_id))) AS review_id,
        LOWER(LTRIM(RTRIM(order_id))) AS order_id,
        CAST(review_score AS INT) AS review_score,
        NULLIF(LTRIM(RTRIM(review_comment_title)), '') AS review_comment_title,
        NULLIF(LTRIM(RTRIM(review_comment_message)), '') AS review_comment_message,
        CAST(review_creation_date AS DATETIME2) AS review_creation_date,
        CAST(review_answer_timestamp AS DATETIME2) AS review_answer_timestamp,
        SYSUTCDATETIME()

    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                    PARTITION BY review_id
                    ORDER BY _ingestion_datetime DESC
               ) AS rn
        FROM bronze.olist_order_reviews
     ) t
    WHERE rn = 1;

    -- ------------------------------
    -- Load orders
    -- ------------------------------
    TRUNCATE TABLE silver.olist_orders;

    INSERT INTO silver.olist_orders (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        timestamp_anomaly_flag,
        delivery_missing_flag,
        _ingestion_datetime
    )

    SELECT
        LOWER(LTRIM(RTRIM(order_id))) AS order_id,
        LOWER(LTRIM(RTRIM(customer_id))) AS customer_id,
        LOWER(LTRIM(RTRIM(order_status))) AS order_status,
        CAST(order_purchase_timestamp AS DATETIME2),
        CAST(order_approved_at AS DATETIME2),
        CAST(order_delivered_carrier_date AS DATETIME2),
        CAST(order_delivered_customer_date AS DATETIME2),
        CAST(order_estimated_delivery_date AS DATETIME2),
        /* Timestamp anomaly flag */
        CASE
            WHEN order_approved_at < order_purchase_timestamp
              OR order_delivered_carrier_date < order_approved_at
              OR order_delivered_customer_date < order_delivered_carrier_date
            THEN 1
            ELSE 0
        END AS timestamp_anomaly_flag,
        /* Delivered but missing delivery date */
        CASE
            WHEN order_status = 'delivered'
                 AND order_delivered_customer_date IS NULL
            THEN 1
            ELSE 0
        END AS delivery_missing_flag,
        SYSUTCDATETIME()

    FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY order_id
                       ORDER BY _ingestion_datetime DESC
                   ) AS rn
            FROM bronze.olist_orders
         ) t

    WHERE rn = 1;


    -- ------------------------------
    -- Load Sellers
    -- ------------------------------
    TRUNCATE TABLE silver.olist_sellers;

    INSERT INTO silver.olist_sellers (
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _ingestion_datetime
    )
    SELECT
        LOWER(LTRIM(RTRIM(seller_id))) AS seller_id,
        RIGHT('00000' + ISNULL(LTRIM(RTRIM(seller_zip_code_prefix)), ''), 5) AS seller_zip_code_prefix,
        LOWER(REPLACE(LTRIM(RTRIM(seller_city)),'-', ' ')) AS seller_city,
        UPPER(LTRIM(RTRIM(seller_state))) AS seller_state,
        SYSUTCDATETIME() AS _ingestion_datetime
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY seller_id
                ORDER BY _ingestion_datetime DESC
            ) AS rn
        FROM bronze.olist_sellers
        WHERE seller_id IS NOT NULL
          AND LEN(LTRIM(RTRIM(seller_state))) = 2      -- State format validation
          AND TRY_CAST(seller_zip_code_prefix AS INT) IS NOT NULL  -- ZIP numeric validation
    ) t
    WHERE rn = 1;

    -- ----------------------------------
    -- Load Product Category Translation
    -- ----------------------------------
    TRUNCATE TABLE silver.products_category_name_translation;

    INSERT INTO silver.products_category_name_translation (
        product_category_name,
        product_category_name_english,
        _ingestion_datetime
    )
    SELECT
        LOWER(LTRIM(RTRIM(product_category_name))) AS product_category_name,
        LOWER(LTRIM(RTRIM(product_category_name_english))) AS product_category_name_english,
        SYSUTCDATETIME() AS _ingestion_datetime
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY product_category_name
                ORDER BY _ingestion_datetime DESC
            ) AS rn
        FROM bronze.products_category_name_translation
        WHERE product_category_name IS NOT NULL
          AND product_category_name_english IS NOT NULL
    ) t
    WHERE rn = 1;


    -- ----------------------------------
    -- Load Products
    -- ----------------------------------

    TRUNCATE TABLE silver.olist_products;

    INSERT INTO silver.olist_products (
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    dimension_anomaly_flag,
    _ingestion_datetime
)
SELECT
    LOWER(LTRIM(RTRIM(product_id))),
    COALESCE(NULLIF(LOWER(LTRIM(RTRIM(product_category_id))), ''), 'uncategorised'),
    COALESCE(CAST(TRY_CAST(product_name_lenght AS DECIMAL(10,3)) AS INT), -1) AS product_name_length,
    COALESCE(CAST(TRY_CAST(product_description_lenght AS DECIMAL(10,3)) AS INT), -1) AS product_description_length,
    COALESCE(CAST(TRY_CAST(product_photos_qty AS DECIMAL(10,3)) AS INT), -1) AS product_photos_qty,
    CAST(product_weight_g AS DECIMAL(10,3)),
    CAST(product_length_cm AS DECIMAL(10,3)),
    CAST(product_height_cm AS DECIMAL(10,3)),
    CAST(product_width_cm AS DECIMAL(10,3)),
    CASE
        WHEN TRY_CAST(product_weight_g AS DECIMAL(10,3)) <= 0
          OR TRY_CAST(product_length_cm AS DECIMAL(10,3)) <= 0
          OR TRY_CAST(product_height_cm AS DECIMAL(10,3)) <= 0
          OR TRY_CAST(product_width_cm AS DECIMAL(10,3)) <= 0
          OR TRY_CAST(product_photos_qty AS INT) < 0
          OR product_category_id IS NULL
        THEN 1
        ELSE 0
    END AS dimension_anomaly_flag,
    SYSUTCDATETIME()
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY product_id
               ORDER BY _ingestion_datetime DESC
           ) AS rn
    FROM bronze.olist_products
) t
WHERE rn = 1;

END

EXEC silver.load_silver

