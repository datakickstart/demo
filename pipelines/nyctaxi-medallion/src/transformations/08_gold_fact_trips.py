"""Gold: fact_trips — trip-grain fact table of the star schema.

Dataset type: MATERIALIZED VIEW.
The fact is the join point of the star, and it must stay referentially consistent
with four dimensions that are themselves MVs recomputed from silver. An MV
recomputes (or incrementally refreshes) alongside them, so a dimension edit —
adding an attribute, re-bucketing time of day — is picked up atomically. A
streaming table would freeze rows written under older dimension logic and would
have to be full-refreshed (destroying streaming state) to catch up.

Foreign keys are computed with the SAME deterministic xxhash64 expressions the
dimensions use, so no join is needed to resolve them and the keys can never drift.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver_schema")
GOLD = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.fact_trips",
    comment=(
        "Trip-grain fact table with FKs to dim_date, dim_zone (pickup + dropoff), "
        "dim_time_of_day and dim_rider, plus trip measures and quality flags."
    ),
    cluster_by=["date_sk", "pickup_zone_sk"],
)
def gold_fact_trips():
    return spark.read.table(f"{CATALOG}.{SILVER}.trips_enriched").select(
        # Degenerate dimension / grain key
        F.col("trip_key").alias("trip_sk"),
        # Foreign keys
        F.xxhash64(F.col("pickup_date").cast("string")).alias("date_sk"),
        F.xxhash64(F.col("pickup_zip").cast("string")).alias("pickup_zone_sk"),
        F.xxhash64(F.col("dropoff_zip").cast("string")).alias("dropoff_zone_sk"),
        F.xxhash64(F.col("time_of_day")).alias("time_of_day_sk"),
        F.xxhash64(F.col("rider_id")).alias("rider_sk"),
        # Natural keys kept for convenience / debugging
        F.col("pickup_date"),
        F.col("pickup_zip"),
        F.col("dropoff_zip"),
        F.col("time_of_day"),
        F.col("rider_id"),
        F.col("tpep_pickup_datetime"),
        F.col("tpep_dropoff_datetime"),
        # Measures
        F.col("fare_amount"),
        F.col("trip_distance"),
        F.col("trip_duration_minutes"),
        F.col("fare_per_mile"),
        # Flags
        # is_cross_borough is NULL when the ZIP-range proxy cannot place an end of
        # the trip; is_region_known says whether the proxy applied at all.
        F.col("is_cross_borough"),
        F.col("is_region_known"),
        F.col("is_invalid_time_order"),
    )
