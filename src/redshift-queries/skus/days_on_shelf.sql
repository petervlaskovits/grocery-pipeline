WITH calculation AS (
    SELECT
        sku,
        AVG(days_on_shelf * 1.0) AS "Average Days On Shelf"
    FROM fact_inventory
    GROUP BY sku
)

SELECT 
    c.sku,
    prod.product_name,
    cat.category_name,
    c."Average Days On Shelf"
FROM calculation c
JOIN dim_product prod
ON c.sku = prod.sku
JOIn dim_category cat
ON prod.category_id = cat.category_id
ORDER BY "Average Days On Shelf" ASC;