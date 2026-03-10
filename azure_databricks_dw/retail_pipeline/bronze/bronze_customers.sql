-- Bronze: Raw customer records from JSON volume
-- Schema evolution enabled via mode=PERMISSIVE; _rescued_data captures any unparseable fields.
CREATE OR REFRESH STREAMING TABLE main.bronze_retail_dev.bronze_customers
  CLUSTER BY (customer_id)
  COMMENT 'Raw customer records ingested from /Volumes/main/demo_dw_raw/raw_data/customers/. Schema evolution enabled.'
AS
SELECT
  *,
  current_timestamp()                  AS _ingested_at,
  _metadata.file_path                  AS _source_file,
  _metadata.file_modification_time     AS _file_modified_at,
  _metadata.file_size                  AS _file_size_bytes
FROM STREAM read_files(
  '/Volumes/main/demo_dw_raw/raw_data/customers/',
  format      => 'json',
  schemaHints => 'customer_id STRING, name STRING, email STRING, membership_level STRING, region STRING',
  mode        => 'PERMISSIVE'
);
