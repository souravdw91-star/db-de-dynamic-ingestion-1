-- =============================================================================
-- Databricks Medallion Architecture - Gold Layer: Star Schema Facts & Dimensions
-- Target Database: gold_db
-- Source Database: silver_db
-- =============================================================================

CREATE DATABASE IF NOT EXISTS gold_db;

-- -----------------------------------------------------------------------------
-- 1. Dimension Table: gold_db.dim_customers
-- Description: Customer profile with order history metrics
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.dim_customers AS
SELECT 
    c.customer_id,
    c.customer_name,
    MIN(o.ordered_at) AS first_order_at,
    MAX(o.ordered_at) AS last_order_at,
    COUNT(o.order_id) AS total_orders_count,
    COALESCE(SUM(o.order_total_cents), 0) AS total_spend_cents
FROM silver_db.customers c
LEFT JOIN silver_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;

-- -----------------------------------------------------------------------------
-- 2. Dimension Table: gold_db.dim_products
-- Description: Product catalogue with price in cents and USD
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.dim_products AS
SELECT 
    sku AS product_sku,
    product_name,
    product_type,
    price_cents,
    CAST(price_cents / 100.0 AS DECIMAL(10,2)) AS price_usd,
    description
FROM silver_db.products;

-- -----------------------------------------------------------------------------
-- 3. Dimension Table: gold_db.dim_stores
-- Description: Store locations and tax rates
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.dim_stores AS
SELECT 
    store_id,
    store_name,
    opened_at,
    tax_rate
FROM silver_db.stores;

-- -----------------------------------------------------------------------------
-- 4. Dimension Table: gold_db.dim_supplies
-- Description: Raw materials and supplies per product SKU
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.dim_supplies AS
SELECT 
    supply_id,
    supply_name,
    cost_cents,
    CAST(cost_cents / 100.0 AS DECIMAL(10,2)) AS cost_usd,
    is_perishable,
    product_sku
FROM silver_db.supplies;

-- -----------------------------------------------------------------------------
-- 5. Fact Table: gold_db.fact_orders
-- Description: Order transaction facts with currency conversions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.fact_orders AS
SELECT 
    order_id,
    customer_id,
    store_id,
    ordered_at,
    CAST(ordered_at AS DATE) AS order_date,
    subtotal_cents,
    tax_paid_cents,
    order_total_cents,
    CAST(subtotal_cents / 100.0 AS DECIMAL(10,2)) AS subtotal_usd,
    CAST(tax_paid_cents / 100.0 AS DECIMAL(10,2)) AS tax_paid_usd,
    CAST(order_total_cents / 100.0 AS DECIMAL(10,2)) AS order_total_usd
FROM silver_db.orders;

-- -----------------------------------------------------------------------------
-- 6. Fact Table: gold_db.fact_order_items
-- Description: Order line item detail facts
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE gold_db.fact_order_items AS
SELECT 
    item_id,
    order_id,
    sku AS product_sku
FROM silver_db.order_items;
