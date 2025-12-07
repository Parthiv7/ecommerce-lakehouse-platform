# Databricks notebook source
# MAGIC %md
# MAGIC ### 02_silver_transform 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup And Config

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, lower, to_timestamp, to_date,
    unix_timestamp, when, coalesce, lit
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### helpers

# COMMAND ----------

def bronze(name):
    return spark.table(f"ecom_catalog.bronze.{name}")

def write_silver(df, name, partition_col=None):
    table = f"ecom_catalog.silver.{name}"
    print(f"Saving Silver Table: {table}")
    if partition_col:
        df.write.format("delta").mode("overwrite").partitionBy(partition_col).option("overwriteSchema", "true").saveAsTable(table)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Saved: {table}")
    display(df.limit(5))

# --- ID CLEANING FUNCTION ---
# It forces everything to a standard integer-string format.
def clean_id(col_name):
    return F.col(col_name).cast("double").cast("long").cast("string")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) customers_clean

# COMMAND ----------

cust = bronze("customers")

customers_clean = cust.select(
    clean_id("customer_id").alias("customer_id"), # Clean ID
    trim(col("name")).alias("name"),
    lower(trim(col("email"))).alias("email"),
    trim(col("country")).alias("country"),
    col("age").cast("int").alias("age"),
    to_date(col("signup_date")).alias("signup_date"),
    col("marketing_opt_in"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
).filter(col("customer_id").isNotNull())

# dedupe by customer_id - keep latest ingest
customers_clean = customers_clean.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("customer_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(customers_clean, "customers_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  2) events_clean

# COMMAND ----------

ev = bronze("events")

events_clean = ev.select(
    clean_id("event_id").alias("event_id"),       # Clean ID
    clean_id("session_id").alias("session_id"),   # Clean ID
    to_timestamp(col("timestamp")).alias("event_ts"),
    trim(col("event_type")).alias("event_type"),
    clean_id("product_id").alias("product_id"),   # Clean ID (Crucial for Join)
    col("qty").cast("int").alias("qty"),
    col("cart_size").cast("int").alias("cart_size"),
    col("payment"),
    col("discount_pct").cast("double").alias("discount_pct"),
    col("amount_usd").cast("double").alias("amount_usd"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
).withColumn("event_date", to_date(col("event_ts"))) \
 .filter(col("event_id").isNotNull())

# dedupe events
events_clean = events_clean.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("event_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(events_clean, "events_clean", partition_col="event_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) orders_clean

# COMMAND ----------

ord_bronze = bronze("orders")

orders_clean = ord_bronze.select(
    clean_id("order_id").alias("order_id"),       # Clean ID
    clean_id("customer_id").alias("customer_id"), # Clean ID
    to_timestamp(col("order_time")).alias("order_ts"),
    trim(col("payment_method")).alias("payment_method"),
    col("discount_pct").cast("double").alias("discount_pct"),
    col("subtotal_usd").cast("double").alias("subtotal_usd"),
    col("total_usd").cast("double").alias("total_usd"),
    trim(col("country")).alias("country"),
    trim(col("device")).alias("device"),
    trim(col("source")).alias("source"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
).withColumn("order_date", to_date(col("order_ts"))) \
 .filter(col("order_id").isNotNull())

# dedupe orders
orders_clean = orders_clean.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("order_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(orders_clean, "orders_clean", partition_col="order_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) order_items_clean

# COMMAND ----------

oi = bronze("order_items")

order_items_clean = oi.select(
    clean_id("order_id").alias("order_id"),     # Clean ID
    clean_id("product_id").alias("product_id"), # Clean ID
    col("unit_price_usd").cast("double").alias("unit_price_usd"),
    col("quantity").cast("int").alias("quantity"),
    col("line_total_usd").cast("double").alias("line_total_usd"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
).filter(col("order_id").isNotNull()) \
 .filter(col("quantity") > 0)

# join to orders_clean to get order_date
orders_df = spark.table("ecom_catalog.silver.orders_clean")

order_items_with_date = order_items_clean.join(
    orders_df.select("order_id", "order_date"),
    on="order_id",
    how="left"
).withColumn("order_date", coalesce(col("order_date"), to_date(col("_ingest_ts"))))

# dedupe order_items (composite key)
order_items_with_date = order_items_with_date.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("order_id", "product_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(order_items_with_date, "order_items_clean", partition_col="order_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) products_clean

# COMMAND ----------

prod = bronze("products")

products_clean = prod.select(
    clean_id("product_id").alias("product_id"), # Clean ID (Crucial target for Joins)
    trim(col("category")).alias("category"),
    trim(col("name")).alias("product_name"),
    col("price_usd").cast("double").alias("price_usd"),
    col("cost_usd").cast("double").alias("cost_usd"),
    col("margin_usd").cast("double").alias("margin_usd"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
)

# dedupe products
products_clean = products_clean.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("product_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(products_clean, "products_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) reviews_clean

# COMMAND ----------

rev = bronze("reviews")

reviews_clean = rev.select(
    clean_id("review_id").alias("review_id"),   # Clean ID
    clean_id("order_id").alias("order_id"),     # Clean ID
    clean_id("product_id").alias("product_id"), # Clean ID
    col("rating").cast("int").alias("rating"),
    col("review_text"),
    to_timestamp(col("review_time")).alias("review_ts"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
).filter(col("review_id").isNotNull())

# dedupe reviews
reviews_clean = reviews_clean.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("review_id").orderBy(col("_ingest_ts").desc()))
).filter(col("_rn") == 1).drop("_rn")

write_silver(reviews_clean, "reviews_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) sessions_clean

# COMMAND ----------

sess = bronze("sessions")

sessions_clean = sess.select(
    clean_id("session_id").alias("session_id"),   # Clean ID
    clean_id("customer_id").alias("customer_id"), # Clean ID
    to_timestamp(col("start_time")).alias("session_start_ts"),
    trim(col("device")).alias("device"),
    trim(col("source")).alias("source"),
    trim(col("country")).alias("country"),
    col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
    col("_source_file")
)

write_silver(sessions_clean, "sessions_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary counts

# COMMAND ----------

tbls = [
    "customers_clean", "events_clean", "orders_clean",
    "order_items_clean", "products_clean", "reviews_clean", "sessions_clean"
]

print("====== SILVER TABLE ROW COUNTS ======")
for t in tbls:
    try:
        print(t, spark.table(f"ecom_catalog.silver.{t}").count())
    except Exception as e:
        print(f"{t}: ERROR ->", e)

print("Silver Layer Transformation Complete (IDs Normalized)")

