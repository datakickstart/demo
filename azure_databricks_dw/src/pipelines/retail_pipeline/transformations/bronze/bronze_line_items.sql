-- Bronze: Raw line item ingestion from JSON volume.
-- Schema evolution enabled (PERMISSIVE mode + schemaHints).
-- Adds ingest metadata: _ingested_at, _source_file, _file_modified_at, _file_size_bytes.
-- Parameters: ${catalog}, ${raw_schema}, ${bronze_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${bronze_schema}.bronze_line_items
  CLUSTER BY (order_id)
  COMMENT 'Raw line item records ingested from /Volumes/${catalog}/${raw_schema}/raw_data/line_items/. Schema evolution enabled.'
AS
SELECT
  *,
  current_timestamp()                  AS _ingested_at,
  _metadata.file_path                  AS _source_file,
  _metadata.file_modification_time     AS _file_modified_at,
  _metadata.file_size                  AS _file_size_bytes
FROM STREAM read_files(
  '/Volumes/${catalog}/${raw_schema}/raw_data/line_items/',
  format      => 'json',
  schemaHints => 'line_item_id STRING, order_id STRING, product_name STRING, quantity INT, unit_price DOUBLE',
  mode        => 'PERMISSIVE'
);
