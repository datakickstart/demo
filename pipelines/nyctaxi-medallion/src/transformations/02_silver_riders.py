"""Silver: synthetic rider dimension (~500 rows) with fully fake PII.

Dataset type: MATERIALIZED VIEW.
This is a generated reference dimension, not an incrementally-arriving feed, so
there is no stream to read and nothing to append. Every value is a pure
deterministic function of the row ordinal (`spark.range` + `xxhash64` with
per-column salts), so a full recompute is idempotent: the same 500 rows with the
same PII come back on every refresh. That is exactly the semantics of an MV.

PII SAFETY: nothing here is real. Names/domains/ZIPs come from fixed literal
lists, phone numbers all use the 555 fictional-use exchange, and card_last4 is a
hash digit string. No real person, address, phone, or card is referenced. This
follows the `databricks-synthetic-data-gen` deterministic-hashing pattern rather
than Faker so the output is stable across refreshes with no extra dependency on
serverless pipeline compute.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver_schema")
RIDER_COUNT = int(spark.conf.get("rider_count"))

FIRST_NAMES = [
    "Ava", "Noah", "Mia", "Liam", "Zoe", "Ethan", "Maya", "Lucas",
    "Nina", "Omar", "Priya", "Diego", "Hana", "Jonas", "Leila", "Marcus",
    "Sofia", "Tariq", "Elena", "Kai", "Ruth", "Andre", "Iris", "Felix",
    "Nadia", "Sean", "Yara", "Victor", "Chloe", "Amir", "Greta", "Pablo",
]
LAST_NAMES = [
    "Alvarez", "Bennett", "Castillo", "Duval", "Eriksen", "Fontaine",
    "Goldberg", "Haddad", "Ibrahim", "Jensen", "Kowalski", "Lindqvist",
    "Moreau", "Nakamura", "Okonkwo", "Petrov", "Quintero", "Rasmussen",
    "Silva", "Tanaka", "Ubbink", "Vasquez", "Whitfield", "Xiong",
    "Yamamoto", "Zielinski", "Ashford", "Beaumont", "Calloway", "Delacroix",
]
EMAIL_DOMAINS = ["example.com", "example.org", "example.net", "mailinator.example"]
NYC_AREA_CODES = ["212", "332", "347", "646", "718", "917", "929"]
# Manhattan / Brooklyn / Queens / Bronx / Staten Island ZIPs present in the sample data.
HOME_ZIPS = [
    "10001", "10011", "10016", "10019", "10023", "10025", "10036", "10065",
    "10128", "10282", "10301", "10451", "10462", "11201", "11215", "11222",
    "11237", "11354", "11375", "11433",
]


def _pick(values, salt, id_col=F.col("id")):
    """Deterministically choose one literal from `values` for each row."""
    idx = F.pmod(F.xxhash64(id_col.cast("string"), F.lit(salt)), F.lit(len(values)))
    # element_at requires an INT position; xxhash64/pmod yields BIGINT.
    return F.element_at(F.array(*[F.lit(v) for v in values]), (idx + 1).cast("int"))


def _digits(salt, n, id_col=F.col("id")):
    """Deterministic zero-padded digit string of length n."""
    modulus = 10**n
    return F.lpad(
        F.pmod(F.xxhash64(id_col.cast("string"), F.lit(salt)), F.lit(modulus)).cast("string"),
        n,
        "0",
    )


@dp.materialized_view(
    name=f"{CATALOG}.{SILVER}.riders",
    comment=(
        "Synthetic rider dimension: 500 deterministically generated, entirely "
        "fake riders. Stable across refreshes; contains no real PII."
    ),
)
def silver_riders():
    first = _pick(FIRST_NAMES, "first_name")
    last = _pick(LAST_NAMES, "last_name")
    return (
        spark.range(0, RIDER_COUNT, numPartitions=8)
        .select(
            F.concat(F.lit("RDR-"), F.lpad(F.col("id").cast("string"), 5, "0")).alias("rider_id"),
            F.concat_ws(" ", first, last).alias("full_name"),
            F.lower(
                F.concat(
                    F.substring(first, 1, 1), F.lit("."), last,
                    _digits("email_num", 2),
                    F.lit("@"), _pick(EMAIL_DOMAINS, "email_domain"),
                )
            ).alias("email"),
            # 555 exchange = reserved fictional range, never a routable number.
            F.concat(
                F.lit("("), _pick(NYC_AREA_CODES, "area_code"), F.lit(") 555-"),
                _digits("phone_line", 4),
            ).alias("phone"),
            _digits("card", 4).alias("card_last4"),
            _pick(HOME_ZIPS, "home_zip").alias("home_zip"),
        )
    )
