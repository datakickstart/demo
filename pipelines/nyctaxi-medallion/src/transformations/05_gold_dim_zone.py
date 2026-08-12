"""Gold: dim_zone (ZIP-grain location dimension).

Dataset type: MATERIALIZED VIEW.
The zone dimension is the DISTINCT union of pickup and dropoff ZIPs across the
whole silver feed — a deduplicating aggregation, which streaming tables cannot
express (append-only). MV also gives correct behaviour if a ZIP later disappears
from silver.

Surrogate key: xxhash64 of the ZIP (the natural key), so zone_sk is stable and
matches the pickup_zone_sk / dropoff_zone_sk computed in fact_trips.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver_schema")
GOLD = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_zone",
    comment=(
        "Location dimension at ZIP grain (samples.nyctaxi.trips only exposes "
        "pickup_zip / dropoff_zip). zone_sk = xxhash64(zip)."
    ),
)
def gold_dim_zone():
    silver = spark.read.table(f"{CATALOG}.{SILVER}.trips_enriched")
    pickups = silver.select(
        F.col("pickup_zip").alias("zip"), F.col("pickup_borough").alias("borough")
    )
    dropoffs = silver.select(
        F.col("dropoff_zip").alias("zip"), F.col("dropoff_borough").alias("borough")
    )
    return (
        pickups.unionByName(dropoffs)
        .where(F.col("zip").isNotNull())
        .distinct()
        .select(
            F.xxhash64(F.col("zip").cast("string")).alias("zone_sk"),
            "zip",
            "borough",
            (F.col("borough") == F.lit("Manhattan")).alias("is_manhattan"),
        )
    )
