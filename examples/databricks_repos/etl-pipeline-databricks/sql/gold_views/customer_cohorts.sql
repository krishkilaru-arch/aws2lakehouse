-- Customer Cohort Metrics (replaces gold.customer_cohort_metrics in Redshift)
-- Now a materialized view in Unity Catalog — auto-refreshed!

CREATE OR REPLACE TABLE production.analytics_gold.customer_cohorts AS
WITH cohorts AS (
    SELECT customer_id, order_cohort_month, order_date,
           DATEDIFF(MONTH, first_order_date, order_date) AS months_since_first
    FROM production.orders_silver.enriched_orders
),
cohort_revenue AS (
    SELECT order_cohort_month AS cohort_month, months_since_first,
           COUNT(DISTINCT customer_id) AS active_customers,
           SUM(line_total) AS cohort_revenue
    FROM cohorts c
    JOIN production.orders_silver.enriched_orders e USING (customer_id, order_date)
    GROUP BY 1, 2
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
    FROM cohorts WHERE months_since_first = 0 GROUP BY 1
)
SELECT cr.*, cs.cohort_size,
       cr.active_customers / cs.cohort_size AS retention_rate
FROM cohort_revenue cr JOIN cohort_sizes cs USING (cohort_month);
