# E-Commerce Clickstream Lakehouse Platform
## Project Summary
This project demonstrates an End-to-End Data Engineering Lakehouse built on Databricks. The pipeline processes approximately 760,000+ clickstream events for 20,000 customers using a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) to generate reliable, high-value business metrics for executive dashboards.

The core focus was building a fault-tolerant Gold layer capable of powering critical BI reports like Customer Lifetime Value (LTV) and Site Conversion Funnels.

## Technology Stack
- Platform : Databricks (Cloud-Native Lakehouse)

- Data Format: Delta Lake (for ACID transactions, schema enforcement)

- Language: PySpark (Python)

- Governance: Unity Catalog (implied by schema structure)

- BI Layer: Databricks SQL (Parameterized Dashboards)

## Data Source
The raw data for this project is sourced from the **E-commerce Transactions + Clickstream** dataset on Kaggle.

**Link:** [E-commerce Transactions + Clickstream Dataset](https://www.kaggle.com/datasets/wafaaelhusseini/e-commerce-transactions-clickstream)

**License:** Creative Commons Attribution-ShareAlike 4.0 International (CC BY-SA 4.0).

## Pipeline Architecture (Medallion Layers)
The repository is organized numerically to reflect the mandatory execution order of the data pipeline:

| Folder | Layer | Data Format | Key Function | Resume Keyword |
| :--- | :--- | :--- | :--- | :--- |
| **01_bronze/** | **BRONZE** | Raw Delta | Simple ingestion of raw source files into immutable, low-cost tables. | Data Ingestion, Landing Zone |
| **02_silver/** | **SILVER** | Cleaned Delta | Data Cleansing, ID Normalization, and Deduplication (e.g., fixing 1050.0 $\to$ 1050). | ETL Transformation, Data Quality |
| **03_gold/** | **GOLD** | Fact/Aggregate | Aggregates cleaned data into business metrics (AOV, Funnels, LTV). Includes all mathematical safety checks. | Business Intelligence, Feature Engineering |
| **04_data_quality/** | **QA GATE** | Queries/Tests | Contains all SQL assertions to verify data integrity and consistency before reporting. | Data Governance, Validation |


## Production Resilience & Technical Challenges Solved
The following issues were identified and resolved to ensure the reliability of the Gold layer:

**1. Event Type Mismatch Correction**  

-**Problem:** The ingestion logic incorrectly mapped clickstream events, searching for "view" when the actual data contained "page_view". This caused view counts to be zero across the entire pipeline.  

-**Solution:** Corrected the product_metrics aggregation in 03_gold/ to explicitly look for event_type == 'page_view', restoring the accurate funnel shape.  

**2. Mathematical Stability**  

-**Problem:** Calculating metrics like Average Order Value (AOV) and conversion rates would return NULL or crash when the denominator (total_orders or view_count) was zero.  

-**Solution:** Implemented CASE WHEN PySpark logic to safely return 0.00 (or F.lit(None)) instead of relying on the problematic F.nullif, ensuring the pipeline never fails and dashboards display clean data.  

**3. Business Logic Handling (The "Quick Add" Anomaly)**  

-**Problem:** Conversion rates sometimes exceeded 100% due to "Quick Add" events (users adding to cart without a page_view).  

-**Solution:** Applied F.least(F.lit(100.0), ...) capping logic in the product_funnel model to maintain visual reporting sanity for stakeholders.  

## Final Dashboard Assets

| S.No | Dashboard Name | Primary Insight & Visualization Type |
| :--- | :--- | :--- |
| **1** | **Product Performance** | Revenue contribution by category and SKU efficiency. (Bar Chart / Table) |
| **2** | **Executive Sales Overview** | Daily Revenue vs. AOV correlation and trend analysis. (Dual-Axis Combo Chart) |
| **3** | **Site-Wide Funnel** | Global conversion flow and drop-off rates (View $\to$ Cart $\to$ Purchase). (Funnel Chart) |
| **4** | **Customer 360** | Identification and segmentation of VIP customers and high-risk "Window Shoppers." (Formatted Table) |



##  GitHub Repository Structure

The project is organized using a clean, professional file structure to align with the Medallion Architecture and ensure clear execution order.

```text
ecommerce-lakehouse-platform/
├── 01_bronze/
│   └── 01_bronze_ingest.py      # Notebook to ingest raw files (Your Bronze Code)
├── 02_silver/
│   └── 02_silver_transform.py # Notebook for cleaning, dedupe, and ID normalization
├── 03_gold/
│   └── 03_gold_aggregate.py      # Notebook to build all Gold tables (Metrics, Funnel, C360)
├── 04_data_quality/
│   └── 04_data_quality_checks.py         # Notebook containing all PASS/FAIL validation checks
├── analysis/
│   ├── dashboard_queries.sql       # All 4 parameterized SQL definitions
│   └── dashboards/
│       ├── 01_product_performance/   # Folder for Product Performance screenshots
│       ├── 02_sales_overview/        # Folder for Sales Overview screenshots
│       ├── 03_conversion_funnel/     # Folder for Funnel screenshots
│       └── 04_customer_360/          # Folder for Customer 360 screenshots
├── assets/
│   ├── raw_data_files/             # sample CSV/XLSX source files
│   └── architecture_diagram.png    # conceptual diagram here
├── .gitignore                      # Python/Databricks ignore list
├── LICENSE                         # Apache 2.0 
└── README.md                       # Project summary, architecture, and setup guide

