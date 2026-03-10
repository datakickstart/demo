-- Silver: Cleaned and enriched customer records.
-- No deduplication — streaming append only.
-- Filters: non-null PK, valid email format, known membership level.
-- Enrichment: lowercase email, membership tier rank ordinal.
-- Parameters: ${catalog}, ${bronze_schema}, ${silver_schema} — set via pipeline configuration.
CREATE OR REFRESH STREAMING TABLE ${catalog}.${silver_schema}.silver_customers
  CLUSTER BY (customer_id)
  COMMENT 'Cleaned customer records streamed from bronze. No dedup. Enriched with tier rank.'
AS
SELECT
  customer_id,
  TRIM(name)                                        AS name,
  LOWER(TRIM(email))                                AS email,
  membership_level,
  region,
  CASE membership_level
    WHEN 'Bronze'   THEN 1
    WHEN 'Silver'   THEN 2
    WHEN 'Gold'     THEN 3
    WHEN 'Platinum' THEN 4
    ELSE 0
  END                                               AS membership_tier_rank,
  _ingested_at,
  _source_file
FROM STREAM ${catalog}.${bronze_schema}.bronze_customers
WHERE customer_id    IS NOT NULL
  AND email          IS NOT NULL
  AND region         IS NOT NULL
  AND membership_level IN ('Bronze', 'Silver', 'Gold', 'Platinum')
  AND _rescued_data  IS NULL;
