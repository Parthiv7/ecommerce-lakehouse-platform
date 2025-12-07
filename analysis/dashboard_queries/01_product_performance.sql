SELECT
    category,
    product_name,
    SUM(view_count) AS total_views,
    SUM(cart_count) AS total_carts,
    SUM(purchase_count) AS total_purchases,
    SUM(total_revenue) AS total_revenue,
    
    --  Use CASE WHEN to handle 0 views (Prevent NULLs)
    CASE 
        WHEN SUM(view_count) = 0 THEN 0.0 
        ELSE ROUND(SUM(cart_count) * 100.0 / SUM(view_count), 2) 
    END AS view_to_cart_rate,
    
    CASE 
        WHEN SUM(cart_count) = 0 THEN 0.0 
        ELSE ROUND(SUM(purchase_count) * 100.0 / SUM(cart_count), 2) 
    END AS cart_to_purchase_rate,
    
    CASE 
        WHEN SUM(view_count) = 0 THEN 0.0 
        ELSE ROUND(SUM(purchase_count) * 100.0 / SUM(view_count), 2) 
    END AS overall_conversion_rate

FROM ecom_catalog.gold.product_metrics
WHERE event_date BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 100;