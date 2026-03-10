-- Gold: Customer dimension — SCD Type 2 (small table, full history tracked).
-- __START_AT / __END_AT added automatically by AUTO CDC.
-- Query current records with: WHERE __END_AT IS NULL
CREATE OR REFRESH STREAMING TABLE main.gold_retail_dev.dim_customers
  CLUSTER BY (customer_id)
  COMMENT 'Customer dimension (SCD Type 2). Full change history. __END_AT IS NULL = current record.';

APPLY CHANGES INTO main.gold_retail_dev.dim_customers
FROM STREAM main.silver_retail_dev.silver_customers
KEYS (customer_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 2;
