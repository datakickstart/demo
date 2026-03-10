-- Gold: Customer dimension — SCD Type 2 (small table, full history tracked).
-- __START_AT / __END_AT added automatically by AUTO CDC.
-- Query current records with: WHERE __END_AT IS NULL
-- Parameters: ${catalog}, ${silver_schema}, ${gold_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${gold_schema}.dim_customers
  CLUSTER BY (customer_id)
  COMMENT 'Customer dimension (SCD Type 2). Full change history. __END_AT IS NULL = current record.';

APPLY CHANGES INTO ${catalog}.${gold_schema}.dim_customers
FROM STREAM ${catalog}.${silver_schema}.silver_customers
KEYS (customer_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 2;
