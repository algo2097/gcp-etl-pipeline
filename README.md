# gcp-etl-pipeline

This project implements an end-to-end ETL pipeline on Google Cloud Platform (GCP) to process Jaguar Land Rover (JLR) vehicle sales and options datasets. The pipeline ingests raw CSVs into a data lake, transforms them into structured BigQuery tables, applies data quality checks, and enriches the sales data with profit calculations.
The system was designed to demonstrate scalable cloud data engineering with layered architecture (RAW → BRONZE → SILVER → GOLD) and orchestration using Airflow (Composer).

## Architecture

          Google Cloud Storage (Raw CSVs)
                       │
                       ▼
            BigQuery External Tables (RAW)
                       │
                       ▼
        PySpark (Databricks) EDA & Cleansing
                       │
                       ▼
           Transformed Tables (BRONZE / SILVER)
                       │
                       ▼
      Enrichment Logic (Profit = Sales - Cost)
                       │
                       ▼
                 Curated Tables (GOLD)
                       │
                       ▼
            Power BI Dashboards

## Tools & Services Used
1. Google Cloud Storage (GCS) → Data Lake for raw & processed files
2. BigQuery → External tables, staging (bronze/silver), curated (gold)
3. PySpark → Data exploration, cleaning, enrichment
4. Databricks → Interactive development & debugging notebooks
5. Cloud Composer (Airflow 3) → Orchestration of pipeline DAGs
6. Great Expectations (style validation) → Data quality checks (duplicates, nulls, schema)
7. Power BI → Dashboarding & reporting


## IAM Policies & Permissions

To enable integration across services, the following key roles were required:

1. BigQuery → roles/bigquery.dataOwner, roles/bigquery.jobUser
2. GCS → roles/storage.objectAdmin
3. Composer → roles/composer.worker
4. Service Accounts → Granted cross-service access for BigQuery, GCS, and Dataproc
