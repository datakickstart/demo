-- Gold: Orders fact — SCD Type 1 (large table, current state only).
-- In-place updates: latest version per order_id is kept; no history rows.
-- Parameters: ${catalog}, ${silver_schema}, ${gold_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${gold_schema}.orders_current
  CLUSTER BY (order_date, status)
  COMMENT 'Current state of all orders (SCD Type 1). Latest record per order_id. No history rows.';

APPLY CHANGES INTO ${catalog}.${gold_schema}.orders_current
FROM STREAM ${catalog}.${silver_schema}.silver_orders
KEYS (order_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;
