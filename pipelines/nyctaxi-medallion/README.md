# nyctaxi-medallion

A Databricks Asset Bundle containing a Lakeflow Spark Declarative Pipeline that builds a
Bronze → Silver → Gold medallion from `samples.nyctaxi.trips` into catalog `main`.

- Compute: **serverless** pipeline compute (`serverless: true`, no cluster/instance config).
- Catalog: `main`. Schemas: `nyctaxi_bronze`, `nyctaxi_silver`, `nyctaxi_gold`.
- Profile: `DEFAULT`.

## Layout

```
databricks.yml                             # bundle + targets (dev default, prod)
resources/nyctaxi_medallion.pipeline.yml   # serverless pipeline resource + configuration
src/transformations/
  01_bronze_trips_raw.py        streaming table  main.nyctaxi_bronze.trips_raw
  02_silver_riders.py           MV               main.nyctaxi_silver.riders
  03_silver_trips_enriched.py   streaming table  main.nyctaxi_silver.trips_enriched
  04_gold_dim_date.py           MV               main.nyctaxi_gold.dim_date
  05_gold_dim_zone.py           MV               main.nyctaxi_gold.dim_zone
  06_gold_dim_time_of_day.py    MV               main.nyctaxi_gold.dim_time_of_day
  07_gold_dim_rider.py          MV               main.nyctaxi_gold.dim_rider
  08_gold_fact_trips.py         MV               main.nyctaxi_gold.fact_trips
```

The pipeline's default publishing target is `main.nyctaxi_bronze`; silver and gold datasets
use fully-qualified `catalog.schema.table` names to publish across the three schemas.
Schema/catalog names and the rider count come from the pipeline `configuration` block and
are read with `spark.conf.get(...)`, so nothing is hard-coded in the transformations.

## Deploy and run

```bash
databricks bundle validate -t dev --profile DEFAULT
databricks bundle deploy   -t dev --profile DEFAULT
databricks bundle run nyctaxi_medallion_etl -t dev --profile DEFAULT
```

## Determinism

Every surrogate key and every synthetic value is a pure function of a natural key:

- `trip_key` = `sha2(pickup_ts|dropoff_ts|distance|fare|pickup_zip|dropoff_zip, 256)` — unique
  across all 21,932 rows of this dataset snapshot (verified empirically, not guaranteed by
  construction: two genuinely identical trips would collide).
- `rider_id` = `RDR-` + `lpad(pmod(xxhash64(trip_key), 500), 5, '0')`.
- Gold surrogate keys = `xxhash64(<natural key>)`; never `monotonically_increasing_id()`.
- Synthetic rider PII = `xxhash64(row ordinal, <per-column salt>)` indexing fixed literal lists.

Proven stable: a `--full-refresh` of the whole pipeline reproduced byte-identical
fingerprints for both the rider dimension and the trip→rider assignment.

## Synthetic PII

`main.nyctaxi_silver.riders` contains **no real PII**. Names, email domains
(`example.com` / `example.org` / `example.net` / `mailinator.example`) and home ZIPs come from
fixed literal lists; all phone numbers use the reserved `555` fictional-use exchange; and
`card_last4` is a hash-derived digit string.
