-- Bronze: Raw order records from JSON volume
-- Schema evolution enabled; _rescued_data captures unparseable fields.
CREATE OR REFRESH STREAMING TABLE main.bronze_retail_dev.bronze_orders
  CLUSTER BY (order_date)
  COMMENT 'Raw order records ingested from /Volumes/main/demo_dw_raw/raw_data/orders/. Schema evolution enabled.'
AS
SELECT
  *,
  current_timestamp()                  AS _ingested_at,
  _metadata.file_path                  AS _source_file,
  _metadata.file_modification_time     AS _file_modified_at,
  _metadata.file_size                  AS _file_size_bytes
FROM STREAM read_files(
  '/Volumes/main/demo_dw_raw/raw_data/orders/',
  format      => 'json',
  schemaHints => 'order_id STRING, customer_id STRING, order_date DATE, total_amount DOUBLE, status STRING',
  mode        => 'PERMISSIVE'
);
