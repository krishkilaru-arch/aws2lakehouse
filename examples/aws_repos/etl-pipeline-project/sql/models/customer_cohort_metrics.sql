-- customer_cohort_metrics.sql
-- Weekly cohort analysis: retention, LTV, revenue contribution by cohort month

CREATE TABLE IF NOT EXISTS gold.customer_cohort_metrics AS
WITH cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', first_order_date) AS cohort_month,
        DATEDIFF('month', first_order_date, order_date) AS months_since_first
    FROM silver.enriched_orders
),
cohort_revenue AS (
    SELECT
        c.cohort_month,
        c.months_since_first,
        COUNT(DISTINCT e.customer_id) AS active_customers,
        SUM(e.line_total) AS cohort_revenue,
        AVG(e.line_total) AS avg_revenue_per_customer
    FROM cohorts c
    JOIN silver.enriched_orders e ON c.customer_id = e.customer_id 
        AND DATEDIFF('month', c.cohort_month, e.order_date) = c.months_since_first
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohorts WHERE months_since_first = 0
    GROUP BY 1
)
SELECT
    cr.*,
    cs.cohort_size,
    cr.active_customers::FLOAT / cs.cohort_size AS retention_rate,
    SUM(cr.cohort_revenue) OVER (PARTITION BY cr.cohort_month ORDER BY cr.months_since_first) AS cumulative_revenue
FROM cohort_revenue cr
JOIN cohort_sizes cs ON cr.cohort_month = cs.cohort_month
ORDER BY cr.cohort_month, cr.months_since_first;
