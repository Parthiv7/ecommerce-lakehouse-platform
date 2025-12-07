-- SALES OVERVIEW: Daily Revenue, AOV, and Traffic
SELECT
    session_date,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(has_purchased) AS total_orders,
    ROUND(SUM(session_revenue), 2) AS total_revenue,
    
    -- Safe AOV Calculation (Revenue / Orders)
    CASE 
        WHEN SUM(has_purchased) = 0 THEN 0.0 
        ELSE ROUND(SUM(session_revenue) / SUM(has_purchased), 2) 
    END AS daily_aov,
    
    -- Safe Session Conversion Rate (Orders / Sessions)
    CASE 
        WHEN COUNT(DISTINCT session_id) = 0 THEN 0.0 
        ELSE ROUND(SUM(has_purchased) * 100.0 / COUNT(DISTINCT session_id), 2) 
    END AS session_conversion_rate

FROM ecom_catalog.gold.session_metrics
WHERE session_date BETWEEN TRY_CAST(:start_date AS DATE) AND TRY_CAST(:end_date AS DATE)
GROUP BY 1
ORDER BY 1 DESC;