
-- Staging tables in Redshift (COPY targets)
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36) NOT NULL,
    order_date DATE NOT NULL,
    subtotal DECIMAL(12,2),
    tax DECIMAL(12,2),
    shipping DECIMAL(12,2),
    order_total DECIMAL(12,2),
    status VARCHAR(20),
    shipping_city VARCHAR(100),
    shipping_state VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) DISTSTYLE KEY DISTKEY(customer_id) SORTKEY(order_date);

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_item_id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36) NOT NULL,
    product_id VARCHAR(36),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_pct DECIMAL(5,4),
    category VARCHAR(50)
) DISTSTYLE KEY DISTKEY(order_id) SORTKEY(order_id);
