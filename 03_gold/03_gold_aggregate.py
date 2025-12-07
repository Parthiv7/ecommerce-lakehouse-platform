# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup And Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

# Define Catalog and Schema
CATALOG_NAME = "ecom_catalog"
SILVER_SCHEMA = f"{CATALOG_NAME}.silver"
GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

# 1. Setup Helper Functions

def silver(name):
    return spark.table(f"{SILVER_SCHEMA}.{name}")

def clean_id(col_name):
    return F.col(col_name).cast("double").cast("long").cast("string")

def write_gold(df, name, partition_col=None):
    table = f"{GOLD_SCHEMA}.{name}"
    print(f"Saving Gold Table: {table}")
    if partition_col:
        df.write.format("delta").mode("overwrite").partitionBy(partition_col).option("overwriteSchema", "true").saveAsTable(table)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    print(f"Saved: {table}")

# Define Window Specification for session duration calculation
SESSION_WINDOW = Window.partitionBy("session_id").orderBy(F.col("event_ts"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Gold Model: product_metrics

# COMMAND ----------




## A. Daily Product Interactions 
event_metrics = silver("events_clean").withColumn(
    "product_id", clean_id("product_id")
).groupBy("event_date", "product_id").agg(
    # FIX: Explicitly check for "page_view" based on your SQL discovery
    F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("view_count"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count")
)

## B. Daily Product Sales (Clean the ID)
order_metrics = silver("order_items_clean").withColumn(
    "product_id", clean_id("product_id") 
).groupBy("order_date", "product_id").agg(
    F.sum("line_total_usd").alias("total_revenue"),
    F.sum("quantity").alias("purchase_count")
).withColumnRenamed("order_date", "event_date")

## C. Product Attributes (Clean the ID)
product_attributes = silver("products_clean").withColumn(
    "product_id", clean_id("product_id")
).select("product_id", "category", "product_name")

## D. Product Review Aggregates (Clean the ID)
review_metrics = silver("reviews_clean").withColumn(
    "product_id", clean_id("product_id")
).groupBy("product_id").agg(
    F.avg("rating").alias("avg_rating"),
    F.count("review_id").alias("review_count")
)

## E. Final Join with Robust Coalescing
gold_product_metrics = event_metrics.join(
    order_metrics,
    ["event_date", "product_id"],
    how="full_outer"
).select(
    F.col("event_date"),
    F.col("product_id"),
    F.coalesce(F.col("view_count"), F.lit(0)).cast("int").alias("view_count"),
    F.coalesce(F.col("cart_count"), F.lit(0)).cast("int").alias("cart_count"),
    F.coalesce(F.col("purchase_count"), F.lit(0)).cast("int").alias("purchase_count"),
    F.coalesce(F.col("total_revenue"), F.lit(0.0)).cast(DoubleType()).alias("total_revenue")
).join(
    product_attributes, ["product_id"], "left"
).join(
    review_metrics, ["product_id"], "left"
).select(
    "event_date",
    "product_id",
    "product_name",
    "category",
    "view_count",
    "cart_count",
    "purchase_count",
    "total_revenue",
    F.round(F.coalesce(F.col("avg_rating"), F.lit(0.0)), 2).alias("avg_rating"),
    F.coalesce(F.col("review_count"), F.lit(0)).cast("int").alias("review_count")
)

# Filter out bad joins
gold_product_metrics = gold_product_metrics.filter(F.col("product_id").isNotNull())

# Write to Gold
write_gold(gold_product_metrics, "product_metrics", partition_col="event_date")





# COMMAND ----------

# MAGIC %md
# MAGIC ###  Gold Model: product_funnel

# COMMAND ----------

# 1. Read product_metrics 
product_metrics_df = spark.table(f"{GOLD_SCHEMA}.product_metrics")

# 2. Calculate Funnel (Capped at 100%)
gold_product_funnel = product_metrics_df.select(
    "event_date",
    "product_id",
    "product_name",
    "category",
    "view_count",
    "cart_count",
    "purchase_count",
    
    # View -> Cart (Capped)
    F.least(
        F.lit(100.0), 
        F.when(F.col("view_count") > 0, F.round((F.col("cart_count") / F.col("view_count")) * 100, 2)).otherwise(F.lit(0.0))
    ).alias("view_to_cart_pct"),
    
    # Cart -> Purchase (Capped)
    F.least(
        F.lit(100.0),
        F.when(F.col("cart_count") > 0, F.round((F.col("purchase_count") / F.col("cart_count")) * 100, 2)).otherwise(F.lit(0.0))
    ).alias("cart_to_purchase_pct"),
    
    # Overall Conversion (Capped)
    F.least(
        F.lit(100.0),
        F.when(F.col("view_count") > 0, F.round((F.col("purchase_count") / F.col("view_count")) * 100, 2)).otherwise(F.lit(0.0))
    ).alias("overall_conversion_pct")
)

write_gold(gold_product_funnel, "product_funnel", partition_col="event_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Model: session_metrics

# COMMAND ----------


# 1. Calculate Session End Time and Aggregated Metrics from events_clean
session_events_agg = silver("events_clean").groupBy("session_id").agg(
    F.min("event_ts").alias("session_start_ts_events"),
    F.max("event_ts").alias("session_end_ts"),
    F.count("event_id").alias("total_events"),
    F.first("event_date").alias("session_date"), 
    # Calculate Session Revenue directly from 'purchase' events
    F.sum(
        F.when(F.col("event_type") == "purchase", F.col("amount_usd")).otherwise(0.0)
    ).alias("session_revenue"),
    F.sum(
        F.when(F.col("event_type") == "purchase", 1).otherwise(0)
    ).alias("has_purchased")
)

# 2. Get Customer & Session Attributes
session_attributes = silver("sessions_clean").select(
    "session_id", "customer_id", "device", "source", "country", "session_start_ts"
).withColumnRenamed("session_start_ts", "session_start_ts_sessions")

# 3. Final Join
gold_session_metrics = session_events_agg.join(
    session_attributes, ["session_id"], "left"
).select(
    F.col("session_id"),
    F.coalesce(F.col("customer_id"), F.lit("UNKNOWN")).alias("customer_id"),
    F.col("session_date"),
    F.coalesce(F.col("session_start_ts_events"), F.col("session_start_ts_sessions")).alias("session_start_ts"),
    F.col("session_end_ts"),
    # Duration Calculation
    (F.col("session_end_ts").cast("long") - 
     F.coalesce(F.col("session_start_ts_events"), F.col("session_start_ts_sessions")).cast("long")
    ).alias("session_duration_sec"),
    F.col("total_events"),
    F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device"),
    F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source"),
    F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country"),
    F.col("session_revenue").cast(DoubleType()),
    F.col("has_purchased").cast("int")
)

# DQ Check: Duration must be >= 0
gold_session_metrics = gold_session_metrics.filter(
    F.col("session_duration_sec").isNotNull() & (F.col("session_duration_sec") >= 0)
)

write_gold(gold_session_metrics, "session_metrics", partition_col="session_date")





# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Model : Customer_360
# MAGIC

# COMMAND ----------

# 1. Aggregate Session Metrics
customer_session_agg = spark.table(f"{GOLD_SCHEMA}.session_metrics").groupBy("customer_id").agg(
    F.sum("session_revenue").alias("customer_total_revenue"),
    F.sum("has_purchased").alias("total_orders"),
    F.countDistinct("session_id").alias("total_sessions"),
    F.max("session_end_ts").alias("last_active_ts"),
    F.min("session_start_ts").alias("first_active_ts")
)

# 2. Get Demographics
customer_demographics = silver("customers_clean").select(
    "customer_id", "name", "email", "country", "age", "signup_date", "marketing_opt_in"
)

# 3. Join
gold_customer_360_base = customer_demographics.join(
    customer_session_agg, ["customer_id"], "left"
)

# 4. Coalesce Metrics (Handle Nulls from Left Join)
gold_customer_360_coalesced = gold_customer_360_base.select(
    F.col("customer_id"), F.col("name"), F.col("email"), F.col("country"), 
    F.col("age"), F.col("signup_date"), F.col("marketing_opt_in"),
    F.coalesce(F.col("customer_total_revenue"), F.lit(0.0)).alias("customer_total_revenue"),
    F.coalesce(F.col("total_orders"), F.lit(0)).cast("int").alias("total_orders"),
    F.coalesce(F.col("total_sessions"), F.lit(0)).cast("int").alias("total_sessions"),
    F.col("first_active_ts"),
    F.col("last_active_ts")
)

# 5. Final Calculation (Safe AOV Division)
gold_customer_360_final = gold_customer_360_coalesced.withColumn(
    "average_order_value",
    F.round(
        F.when(
            gold_customer_360_coalesced["total_orders"] != 0, 
            gold_customer_360_coalesced["customer_total_revenue"] / gold_customer_360_coalesced["total_orders"]
        ).otherwise(F.lit(None).cast(DoubleType())), 
    2)
).withColumn(
    "days_since_signup",
    F.datediff(F.current_date(), F.col("signup_date"))
).withColumn("scd_load_ts", F.current_timestamp())

# Filter valid customers only
gold_customer_360 = gold_customer_360_final.filter(F.col("customer_id").isNotNull())

write_gold(gold_customer_360, "customer_360")