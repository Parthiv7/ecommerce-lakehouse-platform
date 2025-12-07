# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Layer Ingestion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Base volume path for all raw files
volume_base = "/Volumes/ecom_catalog/raw/kaggle_volume/"

files = {
    "customers.csv": "customers",
    "events.csv": "events",
    "orders.csv": "orders",
    "order_items.csv": "order_items",
    "products.csv": "products",
    "reviews.csv": "reviews",
    "sessions.csv": "sessions"
}

for file_name, table_name in files.items():

    print(f"Ingesting {file_name} → Bronze Delta Table {table_name}")

    df = spark.read.option("header", True).option("inferSchema", True).csv(volume_base + file_name)

    df = df.withColumn("_ingest_ts", current_timestamp()) \
           .withColumn("_source_file", lit(file_name))

    # Bronze table fully qualified name
    bronze_table = f"ecom_catalog.bronze.{table_name}"

    df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

    print(f"Created Bronze Table: {bronze_table}")
    display(df.limit(5))


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ecom_catalog.bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecom_catalog.bronze.events LIMIT 20;
# MAGIC

# COMMAND ----------

