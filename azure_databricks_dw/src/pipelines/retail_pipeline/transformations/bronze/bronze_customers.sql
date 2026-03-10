-- Bronze: Raw customer ingestion from JSON volume.
-- Schema evolution enabled (PERMISSIVE mode + schemaHints).
-- Adds ingest metadata: _ingested_at, _source_file, _file_modified_at, _file_size_bytes.
-- Parameters: ${catalog}, ${raw_schema}, ${bronze_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${bronze_schema}.bronze_customers
  CLUSTER BY (customer_id)
  COMMENT 'Raw customer records ingested from /Volumes/${catalog}/${raw_schema}/raw_data/customers/. Schema evolution enabled.'
AS
SELECT
  *,
  current_timestamp()                  AS _ingested_at,
  _metadata.file_path                  AS _source_file,
  _metadata.file_modification_time     AS _file_modified_at,
  _metadata.file_size                  AS _file_size_bytes
FROM STREAM read_files(
  '/Volumes/${catalog}/${raw_schema}/raw_data/customers/',
  format      => 'json',
  schemaHints => 'customer_id STRING, name STRING, email STRING, membership_level STRING, region STRING',
  mode        => 'PERMISSIVE'
);
