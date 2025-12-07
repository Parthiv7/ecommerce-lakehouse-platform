# Databricks notebook source
# MAGIC %md
# MAGIC ### 04_dq_validation
# MAGIC ##### Purpose: Quality Gate - Fails pipeline if bad data is found in Gold

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helper Function: The "Quality Gate"

# COMMAND ----------

def run_dq_check(check_name, failure_query):
    """
    Executes a SQL query that searches for BAD data.
    If the query returns rows, the check FAILS and raises an error.
    """
    print(f"Running Check: {check_name}...")
    
    bad_rows_df = spark.sql(failure_query)
    bad_count = bad_rows_df.count()
    
    if bad_count > 0:
        print(f" FAILURE: {check_name}")
        print(f"   Found {bad_count} bad records.")
        display(bad_rows_df.limit(10))
        # Raise Validation Error to stop the pipeline
        raise ValueError(f"Data Quality Check Failed: {check_name}")
    else:
        print(f" PASS: {check_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEST 1: Product Dimension Integrity (The ID Fix Check)
# MAGIC ##### Rationale: If Silver cleaning worked, there should be NO null names.

# COMMAND ----------

sql_check_product_ids = """
SELECT product_id, product_name, category, view_count 
FROM ecom_catalog.gold.product_metrics
WHERE product_name IS NULL 
   OR category IS NULL
"""
run_dq_check("Gold Product Integrity (No Null Names)", sql_check_product_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEST 2: Customer PK Uniqueness
# MAGIC
# MAGIC ##### Rationale: Customer 360 must be 1:1 with Customer ID.

# COMMAND ----------

sql_check_customer_pk = """
SELECT customer_id, COUNT(*) as dup_count
FROM ecom_catalog.gold.customer_360
GROUP BY customer_id
HAVING COUNT(*) > 1
"""
run_dq_check("Customer 360 PK Uniqueness", sql_check_customer_pk)

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEST 3: Business Logic (Negative Durations/Revenue)
# MAGIC
# MAGIC ##### Rationale: Sessions cannot have negative time or revenue.

# COMMAND ----------

sql_check_negatives = """
SELECT session_id, session_duration_sec, session_revenue
FROM ecom_catalog.gold.session_metrics
WHERE session_duration_sec < 0 
   OR session_revenue < 0
"""
run_dq_check("Sanity Check: No Negative Business Metrics", sql_check_negatives)

# COMMAND ----------

# MAGIC %md
# MAGIC #### TEST 4: Funnel Logic
# MAGIC ##### Rationale: Conversion rates cannot exceed 100%.

# COMMAND ----------


sql_check_funnel = """
SELECT product_id, view_to_cart_pct, cart_to_purchase_pct
FROM ecom_catalog.gold.product_funnel
WHERE view_to_cart_pct > 100 
   OR cart_to_purchase_pct > 100
"""
run_dq_check("Funnel Math Logic (< 100%)", sql_check_funnel)

print("\n ALL DATA QUALITY CHECKS PASSED. DASHBOARDS ARE SAFE TO REFRESH.")