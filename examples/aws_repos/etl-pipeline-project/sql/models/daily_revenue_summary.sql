-- daily_revenue_summary.sql
-- Materialized: daily refresh, partitioned by order_date
-- Dependencies: silver.enriched_orders

CREATE TABLE IF NOT EXISTS gold.daily_revenue_summary AS
WITH order_metrics AS (
    SELECT
        order_date,
        category,
        country,
        acquisition_channel,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(line_total) AS gross_revenue,
        SUM(discount) AS total_discounts,
        SUM(line_total - discount) AS net_revenue,
        SUM(margin) AS total_margin,
        AVG(margin_pct) AS avg_margin_pct,
        SUM(quantity) AS units_sold
    FROM silver.enriched_orders
    WHERE order_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4
),
customer_metrics AS (
    SELECT
        order_date,
        COUNT(DISTINCT CASE WHEN is_first_order THEN customer_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN NOT is_first_order THEN customer_id END) AS returning_customers
    FROM silver.enriched_orders
    WHERE order_date = '{{ ds }}'
    GROUP BY 1
)
SELECT
    o.*,
    c.new_customers,
    c.returning_customers,
    o.net_revenue / NULLIF(o.order_count, 0) AS avg_order_value,
    o.net_revenue / NULLIF(o.unique_customers, 0) AS revenue_per_customer,
    CURRENT_TIMESTAMP AS _calculated_at
FROM order_metrics o
LEFT JOIN customer_metrics c ON o.order_date = c.order_date;
