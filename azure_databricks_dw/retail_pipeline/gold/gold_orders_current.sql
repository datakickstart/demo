-- Gold: Orders fact — SCD Type 1 (large table, current state only).
-- In-place updates: latest version per order_id is kept; no history rows.
CREATE OR REFRESH STREAMING TABLE main.gold_retail_dev.orders_current
  CLUSTER BY (order_date, status)
  COMMENT 'Current state of all orders (SCD Type 1). Latest record per order_id. No history rows.';

APPLY CHANGES INTO main.gold_retail_dev.orders_current
FROM STREAM main.silver_retail_dev.silver_orders
KEYS (order_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;
