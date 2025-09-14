# gcp-etl-pipeline

This project implements an end-to-end ETL pipeline on Google Cloud Platform (GCP) to process Jaguar Land Rover (JLR) vehicle sales and options datasets. The pipeline ingests raw CSVs into a data lake, transforms them into structured BigQuery tables, applies data quality checks, and enriches the sales data with profit calculations.
The system was designed to demonstrate scalable cloud data engineering with layered architecture (RAW → BRONZE → SILVER → GOLD) and orchestration using Airflow (Composer).

          Google Cloud Storage (Raw CSVs)
                       │
                       ▼
            BigQuery External Tables (RAW)
                       │
                       ▼
        PySpark (Dataproc / Databricks) EDA & Cleansing
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
            Power BI / Superset Dashboards
