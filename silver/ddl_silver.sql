IF OBJECT_ID('silver.olist_customers', 'U') IS NOT NULL
	DROP TABLE silver.olist_customers;
GO 
CREATE TABLE silver.olist_customers (
	customer_id NVARCHAR(50),
	customer_unique_id NVARCHAR(50),
	customer_zip_code_prefix NVARCHAR(50),
	customer_city NVARCHAR(100),
	customer_state NVARCHAR(50),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_geolocation', 'U') IS NOT NULL
	DROP TABLE silver.olist_geolocation;
GO
CREATE TABLE silver.olist_geolocation (
	geolocation_zip_code_prefix NVARCHAR(50),
	geolocation_lat DECIMAL(18,16),
	geolocation_lng DECIMAL(18,16),
	geolocation_city NVARCHAR(100),
	geolocation_state NVARCHAR(50),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_order_items', 'U') IS NOT NULL
	DROP TABLE silver.olist_order_items;
GO
CREATE TABLE silver.olist_order_items (
	order_id NVARCHAR(50),
	order_item_id INT,
	product_id NVARCHAR(50),
	seller_id NVARCHAR(50),
	shipping_limit_date DATETIME2,
	price DECIMAL(10,3),
	freight_value DECIMAL(10,3),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_order_payments', 'U') IS NOT NULL
	DROP TABLE silver.olist_order_payments;
GO
CREATE TABLE silver.olist_order_payments (
	order_id NVARCHAR(50),
	payment_sequential INT,
	payment_type NVARCHAR(50),
	payment_installments INT,
	payment_value DECIMAL(10,3),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_order_reviews', 'U') IS NOT NULL
	DROP TABLE silver.olist_order_reviews;
GO
CREATE TABLE silver.olist_order_reviews (
	review_id NVARCHAR(255),
	order_id NVARCHAR(255),
	review_score INT,
	review_comment_title NVARCHAR(MAX),
	review_comment_message NVARCHAR(MAX),
	review_creation_date DATETIME2,
	review_answer_timestamp DATETIME2,

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_orders', 'U') IS NOT NULL
	DROP TABLE silver.olist_orders;
GO
CREATE TABLE silver.olist_orders (
	order_id NVARCHAR(50),
	customer_id NVARCHAR(50),
	order_status NVARCHAR(50),
	order_purchase_timestamp DATETIME2,
	order_approved_at DATETIME2,
	order_delivered_carrier_date DATETIME2,
	order_delivered_customer_date DATETIME2,
	order_estimated_delivery_date DATETIME2,
	timestamp_anomaly_flag INT,
    delivery_missing_flag INT,

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_products', 'U') IS NOT NULL
	DROP TABLE silver.olist_products;
GO
CREATE TABLE silver.olist_products (
	product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,3),
    product_length_cm DECIMAL(10,3),
    product_height_cm DECIMAL(10,3),
    product_width_cm DECIMAL(10,3),
	dimension_anomaly_flag INT,

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.olist_sellers', 'U') IS NOT NULL
	DROP TABLE silver.olist_sellers;
GO 
CREATE TABLE silver.olist_sellers (
	seller_id NVARCHAR(50),
	seller_zip_code_prefix NVARCHAR(50),
	seller_city NVARCHAR(100),
	seller_state NVARCHAR(50),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('silver.products_category_name_translation', 'U') IS NOT NULL
	DROP TABLE silver.products_category_name_translation;
GO
CREATE TABLE silver.products_category_name_translation (
	product_category_name NVARCHAR(255),
	product_category_name_english NVARCHAR(255),

	-- Metadata columns
	_ingestion_datetime DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO