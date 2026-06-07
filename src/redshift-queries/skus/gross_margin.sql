SELECT
    store.store_name AS "Store Location",
    DATE_PART_YEAR(CAST(trans.transaction_date AS DATE)) AS "Year",
    SUM(trans.total_sales_usd) AS "Revenue",
    SUM(prod.unit_cost_usd * trans.quantity_sold) AS "COGS",
    ((revenue - cogs) / revenue) * 100 AS gross_margin
FROM fact_transactions trans 
JOIN dim_product prod
ON trans.sku = prod.sku
JOIN dim_store store
ON trans.store_id = store.store_id
WHERE "Year" IS NOT NULL -- These NULLs could be dates anywhere from 2021 to 2024. If we do not want to have more of these instances pop up, then we should look into debugging the cash register systems or maybe even upgrading them.
GROUP BY store.store_name, "Year"
ORDER BY "Year" DESC;