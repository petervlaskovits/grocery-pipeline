SELECT
    dim_sup.supplier_name,
    DATE_PART_YEAR(CAST(sup.order_date AS DATE)) AS "Year",
    AVG(sup.fill_rate_pct) AS "Average Fill Rate %"
FROM fact_supplier_orders sup
JOIN dim_supplier dim_sup
ON dim_sup.supplier_id = sup.supplier_id
WHERE "Year" IS NOT NULL -- Again, same issue as the Gross Margin issue.
GROUP BY dim_sup.supplier_name, "Year"
ORDER BY "Year" DESC;