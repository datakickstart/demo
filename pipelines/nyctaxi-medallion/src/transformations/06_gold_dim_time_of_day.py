"""Gold: dim_time_of_day.

Dataset type: MATERIALIZED VIEW.
A fixed 4-row lookup generated from literals — there is no incoming stream, so a
streaming table is meaningless here. It is an MV rather than a view so BI tools
join against a stored, physically-small table instead of re-evaluating the
generator on every query.

Surrogate key: xxhash64 of the bucket label, matching fact_trips.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
GOLD = spark.conf.get("gold_schema")

# (label, sort_order, start_hour, end_hour) — must match 03_silver_trips_enriched.py
BUCKETS = [
    ("morning", 1, 6, 11),
    ("afternoon", 2, 12, 17),
    ("evening", 3, 18, 22),
    ("night", 4, 23, 5),
]


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_time_of_day",
    comment="Time-of-day bucket dimension. time_of_day_sk = xxhash64(label).",
)
def gold_dim_time_of_day():
    rows = [(label, order, start, end) for label, order, start, end in BUCKETS]
    return spark.createDataFrame(
        rows, "time_of_day STRING, sort_order INT, start_hour INT, end_hour INT"
    ).select(
        F.xxhash64(F.col("time_of_day")).alias("time_of_day_sk"),
        "time_of_day",
        "sort_order",
        "start_hour",
        "end_hour",
    )
