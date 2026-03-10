-- Gold: Line items fact — SCD Type 1 (large table, current state only).
-- In-place updates: latest version per line_item_id is kept; no history rows.
-- Parameters: ${catalog}, ${silver_schema}, ${gold_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${gold_schema}.line_items_current
  CLUSTER BY (order_id)
  COMMENT 'Current state of all line items (SCD Type 1). Latest record per line_item_id. No history rows.';

APPLY CHANGES INTO ${catalog}.${gold_schema}.line_items_current
FROM STREAM ${catalog}.${silver_schema}.silver_line_items
KEYS (line_item_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;
