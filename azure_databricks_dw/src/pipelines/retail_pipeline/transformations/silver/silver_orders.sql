-- Silver: Cleaned and enriched order records.
-- No deduplication — streaming append only.
-- Enrichment: explicit type casts, calendar fields, order value tier.
-- Parameters: ${catalog}, ${bronze_schema}, ${silver_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_orders
  CLUSTER BY (order_date, status)
  COMMENT 'Cleaned order records streamed from bronze. No dedup. Enriched with calendar & value tier.'
AS
SELECT
  order_id,
  customer_id,
  CAST(order_date   AS DATE)            AS order_date,
  CAST(total_amount AS DECIMAL(12, 2))  AS total_amount,
  status,
  YEAR(CAST(order_date AS DATE))        AS order_year,
  QUARTER(CAST(order_date AS DATE))     AS order_quarter,
  MONTH(CAST(order_date AS DATE))       AS order_month,
  DAYOFWEEK(CAST(order_date AS DATE))   AS order_day_of_week,
  CASE
    WHEN CAST(total_amount AS DECIMAL(12, 2)) <   50  THEN 'micro'
    WHEN CAST(total_amount AS DECIMAL(12, 2)) <  250  THEN 'small'
    WHEN CAST(total_amount AS DECIMAL(12, 2)) < 1000  THEN 'medium'
    ELSE                                               'large'
  END                                   AS order_value_tier,
  _ingested_at,
  _source_file
FROM STREAM ${catalog}.${bronze_schema}.bronze_orders
WHERE order_id      IS NOT NULL
  AND customer_id   IS NOT NULL
  AND order_date    IS NOT NULL
  AND total_amount  > 0
  AND status        IS NOT NULL
  AND _rescued_data IS NULL;
