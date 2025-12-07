-- CUSTOMER 360: VIP & Churn Risk Analysis
SELECT
    customer_id,
    name,
    country,
    total_orders,
    ROUND(customer_total_revenue, 2) AS lifetime_revenue,
    
    -- We already calculated robust AOV in PySpark, so we can just select it
    average_order_value,
    
    days_since_signup,
    CAST(last_active_ts AS DATE) AS last_active_date,
    
    -- Business Segmentation Logic (Dynamic Tiering)
    CASE 
        WHEN customer_total_revenue >= 1000 THEN 'Platinum VIP'
        WHEN customer_total_revenue >= 500 THEN 'Gold Member'
        WHEN total_orders > 0 THEN 'Standard Customer'
        ELSE 'Window Shopper'
    END AS customer_tier

FROM ecom_catalog.gold.customer_360
WHERE 
    -- Filter: Only show customers who were active in the selected window
    CAST(last_active_ts AS DATE) BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)
ORDER BY lifetime_revenue DESC
LIMIT 1000;