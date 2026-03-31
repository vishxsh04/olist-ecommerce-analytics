CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @load_id NVARCHAR(100) = CAST(NEWID() AS NVARCHAR(100));

    BEGIN TRY
        BEGIN TRAN;

        -------------------------------------------------
        -- Customers
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_customers;

        INSERT INTO bronze.olist_customers
        SELECT *,
               SYSUTCDATETIME(),
               'olist_customers_clean.csv',
               @load_id
        FROM raw.olist_customers;

        -------------------------------------------------
        -- Geolocation
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_geolocation;

        INSERT INTO bronze.olist_geolocation
        SELECT *,
               SYSUTCDATETIME(),
               'olist_geolocation_clean.csv',
               @load_id
        FROM raw.olist_geolocation;

        -------------------------------------------------
        -- Order Items
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_order_items;

        INSERT INTO bronze.olist_order_items
        SELECT *,
               SYSUTCDATETIME(),
               'olist_order_items_clean.csv',
               @load_id
        FROM raw.olist_order_items;

        -------------------------------------------------
        -- Order Payments
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_order_payments;

        INSERT INTO bronze.olist_order_payments
        SELECT *,
               SYSUTCDATETIME(),
               'olist_order_payments_clean.csv',
               @load_id
        FROM raw.olist_order_payments;

        -------------------------------------------------
        -- Order Reviews
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_order_reviews;

        INSERT INTO bronze.olist_order_reviews
        SELECT *,
               SYSUTCDATETIME(),
               'olist_order_reviews_clean.csv',
               @load_id
        FROM raw.olist_order_reviews;

        -------------------------------------------------
        -- Orders
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_orders;

        INSERT INTO bronze.olist_orders
        SELECT *,
               SYSUTCDATETIME(),
               'olist_orders_clean.csv',
               @load_id
        FROM raw.olist_orders;

        -------------------------------------------------
        -- Products
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_products;

        INSERT INTO bronze.olist_products
        SELECT *,
               SYSUTCDATETIME(),
               'olist_products_clean.csv',
               @load_id
        FROM raw.olist_products;

        -------------------------------------------------
        -- Sellers
        -------------------------------------------------
        TRUNCATE TABLE bronze.olist_sellers;

        INSERT INTO bronze.olist_sellers
        SELECT *,
               SYSUTCDATETIME(),
               'olist_sellers_clean.csv',
               @load_id
        FROM raw.olist_sellers;

        -------------------------------------------------
        -- Category Translation
        -------------------------------------------------
        TRUNCATE TABLE bronze.products_category_name_translation;

        INSERT INTO bronze.products_category_name_translation
        SELECT *,
               SYSUTCDATETIME(),
               'products_category_name_translation.csv',
               @load_id
        FROM raw.products_category_name_translation;

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        ROLLBACK TRAN;

        THROW;
    END CATCH
END;


EXEC bronze.load_bronze