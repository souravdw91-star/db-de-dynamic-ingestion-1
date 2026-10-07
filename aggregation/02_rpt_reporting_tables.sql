-- =============================================================================
-- Databricks Medallion Architecture - RPT Layer: Business Aggregations & Analytics
-- Target Database: rpt_db
-- Source Database: gold_db
-- =============================================================================

CREATE DATABASE IF NOT EXISTS rpt_db;

-- -----------------------------------------------------------------------------
-- 1. Reporting Table: rpt_db.rpt_monthly_store_performance
-- Description: Monthly store revenue, order volume, and average order value
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE rpt_db.rpt_monthly_store_performance AS
SELECT 
    DATE_FORMAT(fo.ordered_at, 'yyyy-MM') AS year_month,
    ds.store_id,
    ds.store_name,
    COUNT(fo.order_id) AS total_orders,
    SUM(fo.order_total_usd) AS total_revenue_usd,
    CAST(AVG(fo.order_total_usd) AS DECIMAL(10,2)) AS average_order_value_usd,
    SUM(fo.tax_paid_usd) AS total_tax_collected_usd
FROM gold_db.fact_orders fo
JOIN gold_db.dim_stores ds ON fo.store_id = ds.store_id
GROUP BY DATE_FORMAT(fo.ordered_at, 'yyyy-MM'), ds.store_id, ds.store_name
ORDER BY year_month DESC, total_revenue_usd DESC;

-- -----------------------------------------------------------------------------
-- 2. Reporting Table: rpt_db.rpt_customer_lifetime_value
-- Description: Customer Segmentation, Total Spend, and Order Frequency
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE rpt_db.rpt_customer_lifetime_value AS
SELECT 
    dc.customer_id,
    dc.customer_name,
    CAST(dc.first_order_at AS DATE) AS first_order_date,
    CAST(dc.last_order_at AS DATE) AS last_order_date,
    dc.total_orders_count AS total_orders,
    CAST(dc.total_spend_cents / 100.0 AS DECIMAL(12,2)) AS total_lifetime_spend_usd,
    CASE 
        WHEN dc.total_orders_count > 0 THEN CAST((dc.total_spend_cents / 100.0) / dc.total_orders_count AS DECIMAL(10,2))
        ELSE 0.00
    END AS avg_order_value_usd,
    CASE 
        WHEN (dc.total_spend_cents / 100.0) >= 100 THEN 'VIP Customer'
        WHEN (dc.total_spend_cents / 100.0) >= 30 THEN 'Regular Customer'
        WHEN (dc.total_spend_cents / 100.0) > 0 THEN 'Occasional Customer'
        ELSE 'No Purchases'
    END AS customer_segment
FROM gold_db.dim_customers dc
ORDER BY total_lifetime_spend_usd DESC;

-- -----------------------------------------------------------------------------
-- 3. Reporting Table: rpt_db.rpt_product_sales_summary
-- Description: Product Sales Ranking, Units Sold, and Gross Revenue
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE rpt_db.rpt_product_sales_summary AS
SELECT 
    dp.product_sku,
    dp.product_name,
    dp.product_type,
    COUNT(foi.item_id) AS total_units_sold,
    CAST(COUNT(foi.item_id) * dp.price_usd AS DECIMAL(12,2)) AS gross_revenue_usd,
    DENSE_RANK() OVER (ORDER BY COUNT(foi.item_id) * dp.price_usd DESC) AS product_rank_by_revenue
FROM gold_db.dim_products dp
LEFT JOIN gold_db.fact_order_items foi ON dp.product_sku = foi.product_sku
GROUP BY dp.product_sku, dp.product_name, dp.product_type, dp.price_usd
ORDER BY product_rank_by_revenue ASC;
