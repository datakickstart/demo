"""Gold: dim_rider, projected from the silver rider dimension.

Dataset type: MATERIALIZED VIEW.
A dimension projection over a bounded (500-row), fully-recomputable source. The
silver riders MV is regenerated in place rather than appended to, so a streaming
read of it is not meaningful — an MV that recomputes with it keeps the two in
lockstep.

Surrogate key: xxhash64(rider_id). rider_id itself is carried through so the
natural key stays queryable.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver_schema")
GOLD = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{CATALOG}.{GOLD}.dim_rider",
    comment=(
        "Rider dimension projected from silver.riders (synthetic PII only). "
        "rider_sk = xxhash64(rider_id)."
    ),
)
def gold_dim_rider():
    return spark.read.table(f"{CATALOG}.{SILVER}.riders").select(
        F.xxhash64(F.col("rider_id")).alias("rider_sk"),
        "rider_id",
        "full_name",
        "email",
        "phone",
        "card_last4",
        "home_zip",
    )
