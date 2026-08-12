"""Bronze: append-only raw ingest of samples.nyctaxi.trips.

Dataset type: STREAMING TABLE.
The requirement is an append-only raw landing zone. A streaming table is the only
dataset type in Lakeflow that is append-only by construction: each update processes
only newly-arrived source rows (exactly-once, checkpointed by the pipeline) and
appends them, so `_ingested_at` is a true "when did we first see this row" stamp.
A materialized view would recompute the whole result on every refresh and reset
`_ingested_at`, which defeats the purpose of a raw layer.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema")
SOURCE_TABLE = spark.conf.get("source_table")


@dp.table(
    name=f"{CATALOG}.{BRONZE}.trips_raw",
    comment=(
        "Append-only raw ingest of samples.nyctaxi.trips with load-time "
        "_ingested_at and a stable natural-key hash (trip_key)."
    ),
    table_properties={"delta.enableChangeDataFeed": "false"},
)
def bronze_trips_raw():
    return (
        spark.readStream.table(SOURCE_TABLE)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(SOURCE_TABLE))
        # Deterministic natural key: samples.nyctaxi.trips has no primary key. The
        # 6-column tuple is unique across all 21,932 rows of this snapshot (verified
        # empirically, NOT guaranteed by construction — two genuinely identical
        # trips would collide), so this hash is a stable trip identity across full
        # refreshes and re-runs.
        .withColumn(
            "trip_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("tpep_pickup_datetime").cast("string"),
                    F.col("tpep_dropoff_datetime").cast("string"),
                    F.col("trip_distance").cast("string"),
                    F.col("fare_amount").cast("string"),
                    F.col("pickup_zip").cast("string"),
                    F.col("dropoff_zip").cast("string"),
                ),
                256,
            ),
        )
    )
