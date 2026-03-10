-- Bronze: Raw line item records from JSON volume
-- Schema evolution enabled; _rescued_data captures unparseable fields.
CREATE OR REFRESH STREAMING TABLE main.bronze_retail_dev.bronze_line_items
  CLUSTER BY (order_id)
  COMMENT 'Raw line item records ingested from /Volumes/main/demo_dw_raw/raw_data/line_items/. Schema evolution enabled.'
AS
SELECT
  *,
  current_timestamp()                  AS _ingested_at,
  _metadata.file_path                  AS _source_file,
  _metadata.file_modification_time     AS _file_modified_at,
  _metadata.file_size                  AS _file_size_bytes
FROM STREAM read_files(
  '/Volumes/main/demo_dw_raw/raw_data/line_items/',
  format      => 'json',
  schemaHints => 'line_item_id STRING, order_id STRING, product_name STRING, quantity INT, unit_price DOUBLE',
  mode        => 'PERMISSIVE'
);
