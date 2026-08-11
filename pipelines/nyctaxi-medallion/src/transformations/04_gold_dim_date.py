"""Gold: dim_date.

Dataset type: MATERIALIZED VIEW.
A conformed date dimension is a DISTINCT (i.e. aggregating) query over the whole
silver fact feed. Streaming tables are append-only and cannot recompute a
deduplicated result, so an MV with a batch read is the correct type; it also lets
Lakeflow incrementally refresh the dimension as silver grows.

Surrogate key: xxhash64 of the natural key (the date). Deterministic, so the key
for 2016-02-13 is identical on every refresh and in every fact row — unlike
monotonically_increasing_id(), which would re-number rows each run and break
already-loaded facts.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver_schema")
GOLD = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_date",
    comment="Conformed date dimension. date_sk = xxhash64(date natural key).",
)
def gold_dim_date():
    return (
        spark.read.table(f"{CATALOG}.{SILVER}.trips_enriched")
        .select(F.col("pickup_date").alias("date"))
        .where(F.col("date").isNotNull())
        .distinct()
        .select(
            F.xxhash64(F.col("date").cast("string")).alias("date_sk"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.dayofmonth("date").alias("day_of_month"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )
