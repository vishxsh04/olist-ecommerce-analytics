-- Inserting data into raw.olist_Customers
TRUNCATE TABLE raw.olist_customers;

BULK INSERT raw.olist_customers
FROM 'C:\Users\vishe\project_data\olist_customers_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_geolocation
TRUNCATE TABLE raw.olist_geolocation;

BULK INSERT raw.olist_geolocation 
FROM 'C:\Users\vishe\project_data\olist_geolocation_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_order_items
TRUNCATE TABLE raw.olist_order_items;

BULK INSERT raw.olist_order_items 
FROM 'C:\Users\vishe\project_data\olist_order_items_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_order_payments
TRUNCATE TABLE raw.olist_order_payments;

BULK INSERT raw.olist_order_payments
FROM 'C:\Users\vishe\project_data\olist_order_payments_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_order_reviews
TRUNCATE TABLE raw.olist_order_reviews;

BULK INSERT raw.olist_order_reviews
FROM 'C:\Users\vishe\project_data\olist_order_reviews_dataset_clean7.csv' 
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '\t',   -- matches Python sep
    TABLOCK
);
GO
-- Inserting data into raw.olist_orders
TRUNCATE TABLE raw.olist_orders;

BULK INSERT raw.olist_orders
FROM 'C:\Users\vishe\project_data\olist_orders_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_products
TRUNCATE TABLE raw.olist_products

BULK INSERT raw.olist_products
FROM 'C:\Users\vishe\project_data\olist_products_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.olist_sellers
TRUNCATE TABLE raw.olist_sellers

BULK INSERT raw.olist_sellers
FROM 'C:\Users\vishe\project_data\olist_sellers_dataset_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO
-- Inserting data into raw.products_category_name_translation
TRUNCATE TABLE raw.products_category_name_translation;

BULK INSERT raw.products_category_name_translation
FROM 'C:\Users\vishe\project_data\product_category_name_translation_clean.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);