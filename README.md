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

## Data Sources
1. Base Data – Vehicle sales data (~17M rows, VIN, sales price, option codes)
2. Options Data – Option codes with material costs & mapping
3. Vehicle Line Mapping – Metadata mapping platform and nameplate

## IAM Policies & Permissions

To enable integration across services, the following key roles were required:

1. BigQuery → roles/bigquery.dataOwner, roles/bigquery.jobUser
2. GCS → roles/storage.objectAdmin
3. Composer → roles/composer.worker
4. Service Accounts → Granted cross-service access for BigQuery, GCS, and Databricks

## Pipeline Steps

1. Schema Creation – Create datasets (raw, transformed, curated) in BigQuery.
2. Load Raw Extract Tables – External tables from raw CSVs in GCS.
3. Load Raw Tables – Copy into managed BigQuery raw tables.
4. EDA & Cleansing (PySpark) – Handle duplicates, nulls, schema fixes, output to GCS.
5. Load Transformed Extract Tables – External tables from cleaned CSVs.
6. Load Transformed Tables – Into BigQuery transformed schema.
7. Enrichment (PySpark) – Add profit per transaction, material cost averages, enriched base data.
8. Load Enriched Tables – Curated BigQuery tables ready for analytics.
9. Reporting – Connected to Power BI dashboards with KPIs (total profit, cost breakdown, etc.).


## Data Quality Highlights

1. ~18,363 true duplicates removed from Base Data
2. Null handling: 28 VIN nulls, 24 Sales Price nulls
3. 1,209 distinct option codes in Base Data vs 478 in Options Data
4. Vehicle Line Mapping missing values handled


# 🧑‍💻 Author

👤 Sarang Kulkarni <br>
Data Engineer | Cloud ETL | Azure | GCP | AWS <br>
LinkedIn: https://www.linkedin.com/in/sarangkulkarni97/
