-- SITE-WIDE FUNNEL: Aggregating all product interactions
-- We create a UNION structure to make it easy for the Funnel Viz to read

SELECT '1. Product Views' AS funnel_stage, SUM(view_count) AS count 
FROM ecom_catalog.gold.product_metrics 
WHERE event_date BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)

UNION ALL

SELECT '2. Add to Carts' AS funnel_stage, SUM(cart_count) AS count 
FROM ecom_catalog.gold.product_metrics 
WHERE event_date BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)

UNION ALL

SELECT '3. Purchases' AS funnel_stage, SUM(purchase_count) AS count 
FROM ecom_catalog.gold.product_metrics 
WHERE event_date BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)

ORDER BY count DESC;