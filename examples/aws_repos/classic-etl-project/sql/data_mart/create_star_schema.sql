
-- Star schema in Redshift
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id VARCHAR(36),
    customer_key VARCHAR(36),
    date_key DATE,
    geography_key VARCHAR(100),
    item_count INT,
    gross_revenue DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    net_revenue DECIMAL(12,2),
    shipping_revenue DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    is_first_order BOOLEAN,
    order_status VARCHAR(20),
    order_count INT DEFAULT 1
) DISTSTYLE KEY DISTKEY(customer_key) SORTKEY(date_key);

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36),
    lifetime_orders INT,
    lifetime_value DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    customer_tenure_days INT,
    rfm_recency INT,
    value_segment VARCHAR(20),
    activity_status VARCHAR(20)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key VARCHAR(36) PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(100),
    unit_cost DECIMAL(10,2),
    list_price DECIMAL(10,2)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    day_name VARCHAR(10),
    quarter INT,
    week_of_year INT,
    is_weekend BOOLEAN
) DISTSTYLE ALL;
