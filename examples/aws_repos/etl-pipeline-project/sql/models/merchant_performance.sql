-- merchant_performance.sql
-- Gold layer: merchant-level KPIs for the analytics dashboard

SELECT
    merchant_id,
    merchant_name,
    DATE_TRUNC('week', order_date) AS week,
    COUNT(DISTINCT order_id) AS orders,
    SUM(net_revenue) AS revenue,
    AVG(avg_order_value) AS avg_order_value,
    SUM(units_sold) AS units_sold,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END) AS new_customers,
    AVG(margin_pct) AS avg_margin_pct,
    -- Customer acquisition cost approximation
    SUM(discount) / NULLIF(SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END), 0) AS cac_proxy
FROM silver.enriched_orders
GROUP BY 1, 2, 3;
