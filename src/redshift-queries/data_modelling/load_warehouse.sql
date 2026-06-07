-- ================================================================
-- LOAD SCRIPT — run weekly
-- ================================================================

-- ----------------------------------------------------------------
-- Step 1: truncate staging tables and load from S3
-- ----------------------------------------------------------------

TRUNCATE staging_transactions;
TRUNCATE staging_inventory;
TRUNCATE staging_supplier_orders;

COPY staging_transactions
FROM 's3://bucket_name/data/transactions' -- replace with your bucket
IAM_ROLE 'arn:awsrole' -- replace with your IAM_ROLE
REGION 'us-west-1'
FORMAT AS PARQUET;

COPY staging_inventory
FROM 's3://bucket_name/data/inventory'
IAM_ROLE 'arn:awsrole'
REGION 'us-west-1'
FORMAT AS PARQUET;

COPY staging_supplier_orders
FROM 's3://bucket_name/data/suppliers'
IAM_ROLE 'arn:awsrole'
REGION 'us-west-1'
FORMAT AS PARQUET;

-- ----------------------------------------------------------------
-- Step 2: upsert dim_category (SCD Type 1 — stable, no history needed)
-- ----------------------------------------------------------------

INSERT INTO dim_category (category_name)
SELECT DISTINCT category_name
FROM (
    SELECT category_name FROM staging_transactions
    UNION
    SELECT category_name FROM staging_inventory
    UNION
    SELECT category_name FROM staging_supplier_orders -- Gets every category from all of the staging tables and deduplicates them with multiple UNIONs. - Peter
) src
WHERE category_name NOT IN (SELECT category_name FROM dim_category);

-- ----------------------------------------------------------------
-- Step 3: upsert dim_supplier (SCD Type 2)
-- New suppliers get inserted.
-- Changed names expire the old row and insert a new active row.
-- Unchanged suppliers are left alone.
-- ----------------------------------------------------------------

-- 3a: expire rows where the supplier name has changed
UPDATE dim_supplier
SET
    is_current   = FALSE,
    expiry_date  = CURRENT_DATE - 1 -- CURRENT_DATE gets tomorrow's date, so this is necessary...
WHERE is_current = TRUE
  AND supplier_name NOT IN (
      SELECT DISTINCT supplier_name FROM staging_supplier_orders --
  );

-- 3b: insert new or changed supplier names not currently active
INSERT INTO dim_supplier (supplier_name, effective_date, expiry_date, is_current)
SELECT DISTINCT
    src.supplier_name,
    CURRENT_DATE,
    CAST(NULL AS DATE),
    TRUE
FROM (
    SELECT DISTINCT supplier_name FROM staging_supplier_orders
) src
WHERE src.supplier_name NOT IN (
    SELECT supplier_name FROM dim_supplier WHERE is_current = TRUE
);

-- ----------------------------------------------------------------
-- Step 4: upsert dim_store (SCD Type 2)
-- Same pattern as dim_supplier.
-- ----------------------------------------------------------------

-- 4a: expire rows where the store name has changed
UPDATE dim_store
SET
    is_current  = FALSE,
    expiry_date = CURRENT_DATE - 1
WHERE is_current = TRUE
  AND store_name NOT IN (
      SELECT DISTINCT store_name FROM (
          SELECT store_name FROM staging_transactions
          UNION
          SELECT store_name FROM staging_inventory
          UNION
          SELECT store_name FROM staging_supplier_orders
      ) src
  );

-- 4b: insert new or changed store names not currently active
INSERT INTO dim_store (store_name, effective_date, expiry_date, is_current)
SELECT DISTINCT
    src.store_name,
    CURRENT_DATE,
    CAST(NULL AS DATE),
    TRUE
FROM (
    SELECT store_name FROM staging_transactions
    UNION
    SELECT store_name FROM staging_inventory
    UNION
    SELECT store_name FROM staging_supplier_orders
) src
WHERE src.store_name NOT IN (
    SELECT store_name FROM dim_store WHERE is_current = TRUE
);

-- ----------------------------------------------------------------
-- Step 5: upsert dim_product (SCD Type 1)
-- Insert new SKUs, overwrite changed attributes.
-- ----------------------------------------------------------------

INSERT INTO dim_product (sku, product_name, category_id, unit_cost_usd, unit_retail_price_usd)
SELECT DISTINCT
    s.sku,
    s.product_name,
    c.category_id,
    s.unit_cost_usd,
    s.unit_retail_price_usd
FROM staging_inventory s
JOIN dim_category c ON c.category_name = s.category_name
WHERE s.sku NOT IN (SELECT sku FROM dim_product);

UPDATE dim_product
SET
    product_name          = s.product_name,
    unit_cost_usd         = s.unit_cost_usd,
    unit_retail_price_usd = s.unit_retail_price_usd,
    category_id           = c.category_id
FROM (
    SELECT DISTINCT sku, product_name, category_name, unit_cost_usd, unit_retail_price_usd
    FROM staging_inventory
) s
JOIN dim_category c ON c.category_name = s.category_name
WHERE dim_product.sku = s.sku;

-- ----------------------------------------------------------------
-- Step 6: truncate and reload fact tables
-- Join to is_current = TRUE to get the active dimension row
-- ----------------------------------------------------------------

TRUNCATE fact_transactions;
TRUNCATE fact_supplier_orders;
TRUNCATE fact_inventory;

INSERT INTO fact_transactions (
    transaction_id, customer_id, transaction_date, store_id, sku,
    quantity_sold, unit_price_usd, discount_rate, total_sales_usd,
    payment_method, shrinkage_usd, return_flag
)
SELECT
    t.transaction_id,
    t.customer_id,
    t.transaction_date,
    s.store_id,
    t.sku,
    t.quantity_sold,
    t.unit_price_usd,
    t.discount_rate,
    t.total_sales_usd,
    t.payment_method,
    t.shrinkage_usd,
    t.return_flag
FROM staging_transactions t
JOIN dim_store s ON s.store_name = t.store_name AND s.is_current = TRUE;

INSERT INTO fact_supplier_orders (
    order_id, order_date, supplier_id, store_id, sku,
    qty_ordered, qty_received, total_order_value_usd,
    expected_delivery_date, actual_delivery_date,
    fill_rate_pct, invoice_matched_flag
)
SELECT
    o.order_id,
    o.order_date,
    sup.supplier_id,
    st.store_id,
    o.sku,
    o.qty_ordered,
    o.qty_received,
    o.total_order_value_usd,
    o.expected_delivery_date,
    o.actual_delivery_date,
    o.fill_rate_pct,
    o.invoice_matched_flag
FROM staging_supplier_orders o
JOIN dim_supplier sup ON sup.supplier_name = o.supplier_name AND sup.is_current = TRUE
JOIN dim_store    st  ON st.store_name     = o.store_name    AND st.is_current  = TRUE;

INSERT INTO fact_inventory (
    sku, store_id, supplier_id, units_on_hand, reorder_point,
    days_on_shelf, last_restock_date, out_of_stock_days_30d,
    expiry_date, markdown_flag
)
SELECT
    i.sku,
    st.store_id,
    sup.supplier_id,
    i.units_on_hand,
    i.reorder_point,
    i.days_on_shelf,
    i.last_restock_date,
    i.out_of_stock_days_30d,
    i.expiry_date,
    i.markdown_flag
FROM staging_inventory i
JOIN dim_store    st  ON st.store_name     = i.store_name    AND st.is_current  = TRUE
JOIN dim_supplier sup ON sup.supplier_name = i.supplier_name AND sup.is_current = TRUE;