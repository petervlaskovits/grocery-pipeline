WITH calculations AS (
    SELECT
        trans.sku,
        SUM(CASE
            WHEN trans.return_flag = TRUE THEN 1
            ELSE 0
            END
        ) * 1.0 AS total_returns,
        COUNT(*) * 1.0 AS all_sales
    FROM fact_transactions trans
    GROUP BY sku
)

SELECT 
    c.sku,
    prod.product_name,
    cat.category_name, 
    ROUND((total_returns / all_sales) * 100, 2) AS "Return Rate %"
FROM calculations c
JOIN dim_product prod ON c.sku = prod.sku
JOIN dim_category cat ON prod.category_id = cat.category_id
ORDER BY "Return Rate %" DESC;