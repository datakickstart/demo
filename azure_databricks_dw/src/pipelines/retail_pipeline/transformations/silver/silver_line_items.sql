-- Silver: Cleaned and enriched line item records.
-- No deduplication — streaming append only.
-- Enrichment: explicit type casts, computed line_total.
-- Parameters: ${catalog}, ${bronze_schema}, ${silver_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_line_items
  CLUSTER BY (order_id)
  COMMENT 'Cleaned line item records streamed from bronze. No dedup. Adds line_total.'
AS
SELECT
  line_item_id,
  order_id,
  TRIM(product_name)                    AS product_name,
  CAST(quantity   AS INT)               AS quantity,
  CAST(unit_price AS DECIMAL(10, 2))    AS unit_price,
  ROUND(
    CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(10, 2)),
    2
  )                                     AS line_total,
  _ingested_at,
  _source_file
FROM STREAM ${catalog}.${bronze_schema}.bronze_line_items
WHERE line_item_id  IS NOT NULL
  AND order_id      IS NOT NULL
  AND product_name  IS NOT NULL
  AND quantity      > 0
  AND unit_price    > 0
  AND _rescued_data IS NULL;
