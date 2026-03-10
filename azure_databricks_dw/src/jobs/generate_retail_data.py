#!/usr/bin/env python3
"""
Synthetic Retail Order Data Generator

Generates 3 related tables with full referential integrity:
  - customers  (10,000 rows)
  - orders     (50,000 rows)  — total_amount = sum of line_items
  - line_items (≈150,000 rows)

Output: JSON files in /Volumes/{catalog}/{schema}/raw_data/

Usage (Databricks job — catalog/schema passed as task parameters):
  python generate_retail_data.py --catalog main --schema demo_dw_raw

Usage (local with Databricks Connect):
  python generate_retail_data.py --catalog main --schema demo_dw_raw
"""

import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, IntegerType,
    StructType, StructField,
)
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ── Argument Parsing ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Generate synthetic retail data")
parser.add_argument("--catalog", default="main",        help="Unity Catalog name")
parser.add_argument("--schema",  default="demo_dw_raw", help="Schema for raw data volume")
args = parser.parse_args()

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG     = args.catalog
SCHEMA      = args.schema
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"

N_CUSTOMERS = 10_000
N_ORDERS    = 50_000
SEED        = 42

END_DATE        = datetime(2026, 3, 9)
START_DATE      = END_DATE - timedelta(days=365)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days

# ── Spark Session ─────────────────────────────────────────────────────────────
# Tries Databricks Connect first (local dev), falls back to PySpark (Databricks job).
# Library deps for job mode are declared in the job environment config in the bundle.
try:
    from databricks.connect import DatabricksSession, DatabricksEnv
    env = DatabricksEnv().withDependencies("faker", "numpy", "pandas")
    spark = (
        DatabricksSession.builder
        .withEnvironment(env)
        .serverless(True)
        .getOrCreate()
    )
    print("Running with Databricks Connect (local mode)")
except ImportError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    print("Running on Databricks (job mode)")

spark.conf.set("spark.sql.shuffle.partitions", "32")

# ── Infrastructure ────────────────────────────────────────────────────────────
print("Creating infrastructure...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.raw_data")
print(f"  Schema: {CATALOG}.{SCHEMA}")
print(f"  Volume: {VOLUME_PATH}")

# ==============================================================================
# STEP 1: CUSTOMERS
# ==============================================================================
print(f"\n[1/5] Generating {N_CUSTOMERS:,} customers...")

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West", "Northwest"]


@F.pandas_udf(StringType())
def fake_name_udf(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    return pd.Series([fake.name() for _ in range(len(ids))])


@F.pandas_udf(StringType())
def fake_email_udf(ids: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    return pd.Series([fake.email() for _ in range(len(ids))])


customers_df = (
    spark.range(0, N_CUSTOMERS, numPartitions=16)
    .withColumn("rand_val", F.rand(SEED))
    .select(
        F.concat(F.lit("CUST-"), F.lpad(F.col("id").cast("string"), 5, "0")).alias("customer_id"),
        fake_name_udf(F.col("id")).alias("name"),
        fake_email_udf(F.col("id")).alias("email"),
        # Membership: Bronze 50%, Silver 30%, Gold 15%, Platinum 5%
        F.when(F.col("rand_val") < 0.50, "Bronze")
         .when(F.col("rand_val") < 0.80, "Silver")
         .when(F.col("rand_val") < 0.95, "Gold")
         .otherwise("Platinum").alias("membership_level"),
        F.element_at(
            F.array([F.lit(r) for r in REGIONS]),
            (F.abs(F.hash(F.col("id"))) % len(REGIONS) + 1).cast("int"),
        ).alias("region"),
    )
)

# Write to intermediate Delta (required for FK join — never use .cache() on serverless)
customers_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.customers_staging")
print(f"  ✓ {N_CUSTOMERS:,} customers written to staging")

# ==============================================================================
# STEP 2: ORDERS (weighted customer assignment by membership level)
# ==============================================================================
print(f"\n[2/5] Generating {N_ORDERS:,} orders with weighted customer assignment...")

# Collect customer list to driver (10K rows — safe to collect)
customer_pd = (
    spark.table(f"`{CATALOG}`.`{SCHEMA}`.customers_staging")
    .select("customer_id", "membership_level")
    .toPandas()
)

# Higher membership → more orders (Bronze=1, Silver=2, Gold=3, Platinum=5)
MEMBERSHIP_WEIGHTS = {"Bronze": 1, "Silver": 2, "Gold": 3, "Platinum": 5}
customer_pd["weight"] = customer_pd["membership_level"].map(MEMBERSHIP_WEIGHTS)
total_weight = customer_pd["weight"].sum()
customer_pd["prob"] = customer_pd["weight"] / total_weight

cust_ids   = customer_pd["customer_id"].tolist()
cust_probs = customer_pd["prob"].tolist()


@F.pandas_udf(StringType())
def sample_customer_id(ids: pd.Series) -> pd.Series:
    """Weighted random sample — Platinum/Gold customers get proportionally more orders."""
    return pd.Series(np.random.choice(cust_ids, size=len(ids), p=cust_probs))


ORDER_STATUSES = ["Delivered", "Shipped", "Processing", "Pending", "Cancelled", "Returned"]
STATUS_PROBS   = [0.55, 0.20, 0.10, 0.08, 0.05, 0.02]


@F.pandas_udf(StringType())
def sample_status(ids: pd.Series) -> pd.Series:
    return pd.Series(np.random.choice(ORDER_STATUSES, size=len(ids), p=STATUS_PROBS))


# num_items distribution → avg ≈ 3.0 items per order
NUM_ITEMS_VALS  = [1, 2, 3, 4, 5]
NUM_ITEMS_PROBS = [0.15, 0.25, 0.30, 0.20, 0.10]


@F.pandas_udf(IntegerType())
def sample_num_items(ids: pd.Series) -> pd.Series:
    return pd.Series(
        np.random.choice(NUM_ITEMS_VALS, size=len(ids), p=NUM_ITEMS_PROBS).astype(int)
    )


orders_base_df = (
    spark.range(0, N_ORDERS, numPartitions=32)
    .select(
        F.concat(F.lit("ORD-"), F.lpad(F.col("id").cast("string"), 6, "0")).alias("order_id"),
        sample_customer_id(F.col("id")).alias("customer_id"),
        F.date_add(
            F.lit(START_DATE.date()),
            (F.rand(SEED + 10) * DATE_RANGE_DAYS).cast("int"),
        ).alias("order_date"),
        sample_status(F.col("id")).alias("status"),
        sample_num_items(F.col("id")).alias("num_items"),
    )
)

# Write orders staging without total_amount (computed later from line_items)
orders_base_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.orders_staging")
print(f"  ✓ {N_ORDERS:,} orders written to staging (total_amount pending)")

# ==============================================================================
# STEP 3: LINE ITEMS (explode orders, assign product details)
# ==============================================================================
print("\n[3/5] Generating line items by exploding orders...")

# Realistic retail product catalog: (name, base_price)
PRODUCTS = [
    # Electronics
    ("Wireless Earbuds Pro",              89.99),
    ("Smart Watch Series 5",             249.99),
    ("Bluetooth Speaker Mini",            59.99),
    ("USB-C Hub 7-Port",                  49.99),
    ("Laptop Stand Adjustable",           39.99),
    ("Mechanical Keyboard TKL",          129.99),
    ("Webcam 4K Ultra HD",                99.99),
    ("Noise Cancelling Headphones",      199.99),
    ("Portable Charger 20000mAh",         44.99),
    ("Smart LED Strip Lights 16ft",       29.99),
    # Apparel
    ("Premium Cotton T-Shirt",            24.99),
    ("Denim Slim Fit Jeans",              59.99),
    ("Fleece Zip Hoodie",                 44.99),
    ("Athletic Running Shorts",           34.99),
    ("Merino Wool Sweater",               69.99),
    ("Waterproof Shell Jacket",           89.99),
    ("Compression Leggings",              39.99),
    ("Classic Canvas Sneakers",           54.99),
    # Home & Garden
    ("Bamboo Cutting Board Set 3pc",      34.99),
    ("Stainless Steel Water Bottle 32oz", 29.99),
    ("Ceramic Planter Set 3pc",           27.99),
    ("LED Desk Lamp Dimmable",            39.99),
    ("Weighted Blanket 15lb",             64.99),
    ("Memory Foam Pillow Queen",          49.99),
    ("Scented Soy Candle Set",            24.99),
    ("Indoor Herb Garden Kit",            34.99),
    # Kitchen
    ("Air Fryer 5.8 Qt",                  89.99),
    ("Coffee Maker with Grinder",         79.99),
    ("Cast Iron Skillet 12in",            44.99),
    ("Silicone Baking Mat Set 3pc",       19.99),
    ("Instant Pot Duo 6 Qt",              99.99),
    ("Mandoline Slicer Pro",              34.99),
    ("Non-Stick Frying Pan Set",          59.99),
    ("Glass Meal Prep Container 10pc",    39.99),
    # Sports & Outdoors
    ("Yoga Mat Premium Non-Slip",         34.99),
    ("Resistance Band Set 5pc",           24.99),
    ("Insulated Hiking Bottle 40oz",      39.99),
    ("Foam Roller Deep Tissue",           29.99),
    ("Speed Jump Rope Cable",             19.99),
    ("Adjustable Dumbbell Pair 25lb",     79.99),
    ("Trekking Poles Collapsible",        54.99),
    ("Running Hydration Belt",            29.99),
    # Books & Media
    ("Python for Data Engineers",         49.99),
    ("The Art of SQL",                    44.99),
    ("Cloud Architecture Patterns",       54.99),
    ("Machine Learning Fundamentals",     59.99),
    ("Designing Data-Intensive Apps",     54.99),
    # Beauty & Personal Care
    ("Electric Toothbrush Pro Series",    59.99),
    ("Vitamin C Brightening Serum 1oz",   29.99),
    ("Ultrasonic Aromatherapy Diffuser",  34.99),
    ("Natural Charcoal Face Wash",        19.99),
    ("Hyaluronic Acid Moisturizer",       24.99),
    # Toys & Games
    ("Strategy Board Game Deluxe",        44.99),
    ("STEM Building Blocks 500pc",        49.99),
    ("Remote Control 4WD Rock Crawler",   54.99),
    ("Scenic Puzzle 1000 Piece",          19.99),
    ("Watercolor Paint Set 48pc",         34.99),
]

PRODUCT_NAMES  = [p[0] for p in PRODUCTS]
PRODUCT_PRICES = [p[1] for p in PRODUCTS]
N_PRODUCTS     = len(PRODUCTS)

line_item_schema = StructType([
    StructField("product_name", StringType()),
    StructField("quantity",     IntegerType()),
    StructField("unit_price",   DoubleType()),
])


@F.pandas_udf(line_item_schema)
def generate_line_item(order_ids: pd.Series) -> pd.DataFrame:
    """Generate a single line item detail per row."""
    n = len(order_ids)
    product_idx = np.random.randint(0, N_PRODUCTS, size=n)
    quantities  = np.random.choice([1, 2, 3, 4, 5], size=n, p=[0.40, 0.30, 0.15, 0.10, 0.05])
    return pd.DataFrame({
        "product_name": [PRODUCT_NAMES[i] for i in product_idx],
        "quantity":     quantities.astype(int),
        "unit_price":   [round(PRODUCT_PRICES[i], 2) for i in product_idx],
    })


# Explode each order into num_items rows, then attach product details
orders_staging = spark.table(f"`{CATALOG}`.`{SCHEMA}`.orders_staging")

line_items_df = (
    orders_staging
    .withColumn("item_seq", F.sequence(F.lit(1), F.col("num_items")))
    .select(
        "order_id",
        F.posexplode("item_seq").alias("item_pos", "_seq_val"),
    )
    .drop("_seq_val")
    .withColumn("detail", generate_line_item(F.col("order_id")))
    .select(
        F.concat(
            F.col("order_id"),
            F.lit("-LI-"),
            F.lpad((F.col("item_pos") + 1).cast("string"), 2, "0"),
        ).alias("line_item_id"),
        F.col("order_id"),
        F.col("detail.product_name").alias("product_name"),
        F.col("detail.quantity").alias("quantity"),
        F.col("detail.unit_price").alias("unit_price"),
    )
)

line_items_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.line_items_staging")

li_count = spark.table(f"`{CATALOG}`.`{SCHEMA}`.line_items_staging").count()
print(f"  ✓ {li_count:,} line items written to staging")

# ==============================================================================
# STEP 4: Compute order total_amount = sum(quantity * unit_price)
# ==============================================================================
print("\n[4/5] Computing order totals from line items (ensures total_amount integrity)...")

order_totals = (
    spark.table(f"`{CATALOG}`.`{SCHEMA}`.line_items_staging")
    .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    .groupBy("order_id")
    .agg(F.round(F.sum("line_total"), 2).alias("total_amount"))
)

# ==============================================================================
# STEP 5: Write final tables as JSON to volume
# ==============================================================================
print(f"\n[5/5] Writing final JSON files to {VOLUME_PATH}/")

# — CUSTOMERS —
customers_final = spark.table(f"`{CATALOG}`.`{SCHEMA}`.customers_staging") \
    .select("customer_id", "name", "email", "membership_level", "region")
customers_final.write.mode("overwrite").json(f"{VOLUME_PATH}/customers")
print(f"  ✓ customers/     {N_CUSTOMERS:>10,} rows")

# — ORDERS (with total_amount joined from line_items aggregate) —
orders_final = (
    spark.table(f"`{CATALOG}`.`{SCHEMA}`.orders_staging")
    .join(order_totals, on="order_id", how="left")
    .select("order_id", "customer_id", "order_date", "total_amount", "status")
)
orders_final.write.mode("overwrite").json(f"{VOLUME_PATH}/orders")
print(f"  ✓ orders/        {N_ORDERS:>10,} rows")

# — LINE ITEMS —
line_items_final = spark.table(f"`{CATALOG}`.`{SCHEMA}`.line_items_staging") \
    .select("line_item_id", "order_id", "product_name", "quantity", "unit_price")
line_items_final.write.mode("overwrite").json(f"{VOLUME_PATH}/line_items")
print(f"  ✓ line_items/    {li_count:>10,} rows")

# — CLEANUP STAGING TABLES —
print("\nDropping intermediate staging tables...")
for tbl in ("customers_staging", "orders_staging", "line_items_staging"):
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.`{SCHEMA}`.`{tbl}`")
print("  ✓ Staging tables dropped")

# ==============================================================================
# VALIDATION
# ==============================================================================
print("\n" + "=" * 60)
print("GENERATION COMPLETE — VALIDATION")
print("=" * 60)

customers_json  = spark.read.json(f"{VOLUME_PATH}/customers")
orders_json     = spark.read.json(f"{VOLUME_PATH}/orders")
line_items_json = spark.read.json(f"{VOLUME_PATH}/line_items")

print(f"\nRow counts:")
print(f"  customers:  {customers_json.count():>10,}")
print(f"  orders:     {orders_json.count():>10,}")
print(f"  line_items: {line_items_json.count():>10,}")

print("\nMembership level distribution (target: Bronze 50%, Silver 30%, Gold 15%, Platinum 5%):")
customers_json.groupBy("membership_level").count().orderBy("membership_level").show()

print("Orders per membership level (Platinum should have most per customer):")
orders_json.alias("o") \
    .join(customers_json.select("customer_id", "membership_level").alias("c"), on="customer_id") \
    .groupBy("c.membership_level") \
    .agg(
        F.count("*").alias("total_orders"),
        F.countDistinct("o.customer_id").alias("unique_customers"),
        F.round(F.count("*") / F.countDistinct("o.customer_id"), 1).alias("orders_per_customer"),
        F.round(F.avg("o.total_amount"), 2).alias("avg_order_value"),
    ) \
    .orderBy("membership_level").show()

print("Referential integrity check (both should be 0):")
orphan_orders = orders_json \
    .join(customers_json.select("customer_id"), on="customer_id", how="left_anti").count()
orphan_items = line_items_json \
    .join(orders_json.select("order_id"), on="order_id", how="left_anti").count()
print(f"  Orders with invalid customer_id:   {orphan_orders}")
print(f"  Line items with invalid order_id:  {orphan_items}")

print(f"\nJSON files written to: {VOLUME_PATH}/")
print("  customers/    → customer_id, name, email, membership_level, region")
print("  orders/       → order_id, customer_id, order_date, total_amount, status")
print("  line_items/   → line_item_id, order_id, product_name, quantity, unit_price")
