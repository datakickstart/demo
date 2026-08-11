"""Silver: cleaned + enriched trips, derived from the bronze streaming table.

Dataset type: STREAMING TABLE.
Bronze is append-only, and every transformation here is row-local (no aggregation,
no join, no cross-row state). Streaming the bronze append feed means each pipeline
update only processes rows bronze appended since the last update, and the
expectations below are evaluated once per row at ingest instead of on every full
recompute. A materialized view would re-scan and re-derive all 21,932 rows each
refresh for no benefit.

Data quality:
  * DROP (Lakeflow expectations) — non-positive fare_amount or trip_distance.
  * FLAG (plain derived column, NOT an expectation) — is_invalid_time_order for
    rows where dropoff precedes pickup. These rows are kept.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze_schema")
SILVER = spark.conf.get("silver_schema")
RIDER_COUNT = int(spark.conf.get("rider_count"))


def borough_of(zip_col):
    """Map a NYC ZIP code (INT) to a borough name.

    samples.nyctaxi.trips exposes no borough / zone / lat-lon columns — only
    pickup_zip and dropoff_zip (INT). NYC ZIP ranges are borough-contiguous, so
    the ZIP range is a faithful, haversine-free borough proxy.
    """
    return (
        F.when(zip_col.between(10001, 10282), F.lit("Manhattan"))
        .when(zip_col.between(10301, 10314), F.lit("Staten Island"))
        .when(zip_col.between(10451, 10475), F.lit("Bronx"))
        .when(zip_col.between(11201, 11256), F.lit("Brooklyn"))
        .when(zip_col.between(11001, 11109), F.lit("Queens"))
        .when(zip_col.between(11351, 11499), F.lit("Queens"))
        .when(zip_col.between(11690, 11697), F.lit("Queens"))
        .when(zip_col.between(11501, 11599), F.lit("Long Island"))
        .when(zip_col.between(11701, 11980), F.lit("Long Island"))
        .when(zip_col.between(6000, 6999), F.lit("Connecticut"))
        .when(zip_col.between(7000, 8999), F.lit("New Jersey"))
        .otherwise(F.lit("Unknown"))
    )


TIME_OF_DAY_BUCKETS = (
    F.when(F.hour("tpep_pickup_datetime").between(6, 11), F.lit("morning"))
    .when(F.hour("tpep_pickup_datetime").between(12, 17), F.lit("afternoon"))
    .when(F.hour("tpep_pickup_datetime").between(18, 22), F.lit("evening"))
    .otherwise(F.lit("night"))  # 23:00-05:59
)


@dp.table(
    name=f"{CATALOG}.{SILVER}.trips_enriched",
    comment=(
        "Cleaned + enriched trips: duration, fare_per_mile, ZIP-range borough "
        "proxy, time-of-day bucket, deterministic rider_id FK. Rows with "
        "non-positive fare/distance are dropped; reversed timestamps are flagged."
    ),
    cluster_by=["pickup_date", "pickup_zip"],
)
@dp.expect_all_or_drop(
    {
        "positive_fare_amount": "fare_amount > 0",
        "positive_trip_distance": "trip_distance > 0",
    }
)
def silver_trips_enriched():
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    pickup_borough = borough_of(F.col("pickup_zip"))
    dropoff_borough = borough_of(F.col("dropoff_zip"))

    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE}.trips_raw")
        .withColumn("pickup_date", F.to_date(pickup))
        .withColumn(
            "trip_duration_minutes",
            F.round((dropoff.cast("long") - pickup.cast("long")) / 60.0, 3),
        )
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 4))
        .withColumn("pickup_borough", pickup_borough)
        .withColumn("dropoff_borough", dropoff_borough)
        # Cross-borough proxy: ZIP-range-derived borough differs end to end.
        .withColumn("is_cross_borough", pickup_borough != dropoff_borough)
        .withColumn("time_of_day", TIME_OF_DAY_BUCKETS)
        # FLAG, not a drop rule: dropoff before pickup.
        .withColumn("is_invalid_time_order", dropoff < pickup)
        # Deterministic FK: stable hash of the trip natural key, mod rider count.
        # Same trip -> same rider on every refresh (no rand(), no monotonic ids).
        .withColumn(
            "rider_id",
            F.concat(
                F.lit("RDR-"),
                F.lpad(
                    F.pmod(F.xxhash64(F.col("trip_key")), F.lit(RIDER_COUNT)).cast("string"),
                    5,
                    "0",
                ),
            ),
        )
        .select(
            "trip_key",
            "rider_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pickup_date",
            "time_of_day",
            "pickup_zip",
            "dropoff_zip",
            "pickup_borough",
            "dropoff_borough",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
            "fare_per_mile",
            "is_cross_borough",
            "is_invalid_time_order",
            "_ingested_at",
        )
    )
