# Databricks notebook source
# Importing necessry libraries and creating a spark session.
from pyspark.sql import SparkSession, functions as func, types as t, Window
import datetime as dt
import sys
#from google.cloud.dataproc_spark_connect import DataprocSparkSession
#from google.cloud.dataproc_v1 import Session

project  = "enrichment-etl-jlr"
location = "europe-west2"

#session_template = "runtime-fe28"
#session = Session()
#session.session_template = f"projects/{project}/locations/{location}/sessionTemplates/{session_template}"

spark = (SparkSession.builder
         .appName("step4-eda_raw_tables")
         .getOrCreate())

# Setting up some configuration variables.
PROJECT_ID = 'enrichment-etl-jlr'
BUCKET = 'enrichment-etl-jlr'
RUN_DATE = dt.date.today().strftime("%Y%m%d")

# COMMAND ----------

# Loading the BigQuery tables created
base_df = spark.read.format("bigquery").option("table", "enrichment-etl-jlr.raw.base_data").load()
options_df = spark.read.format("bigquery").option("table", "enrichment-etl-jlr.raw.options_data").load()
vehicle_df  = spark.read.format("bigquery").option("table", "enrichment-etl-jlr.raw.vehicle_line_mapping").load()

# Output Directory
OUT_BASE = f"gs://{BUCKET}/transformed/clean/base_data/"
OUT_OPTS = f"gs://{BUCKET}/transformed/clean/options_data/"
OUT_VLM = f"gs://{BUCKET}/transformed/clean/vehicle_line_mapping/"
OUT_BASE_PARQUET = f"gs://{BUCKET}/transformed/clean/base_data_parquet/"
OUT_OPTS_PARQUET = f"gs://{BUCKET}/transformed/clean/options_data_parquet/"
OUT_VLM_PARQUET = f"gs://{BUCKET}/transformed/clean/vehicle_line_mapping_parquet/"

# COMMAND ----------

# True Duplicates in the Dataset

"""
print(f"Initial row count (base_df): {base_df.count()}")
true_duplicates_check = base_df.groupBy(*base_df.columns).count().filter(func.col("count") > 1)
print(f"Number of true duplicate groups found (base_df): {true_duplicates_check.count()}")

print(f"Initial row count (options_df): {options_df.count()}")
true_duplicates_check = options_df.groupBy(*options_df.columns).count().filter(func.col("count") > 1)
print(f"Number of true duplicate groups found (options_df): {true_duplicates_check.count()}")

print(f"Initial row count (vehicle_df): {vehicle_df.count()}")
true_duplicates_check = vehicle_df.groupBy(*vehicle_df.columns).count().filter(func.col("count") > 1)
print(f"Number of true duplicate groups found (vehicle_df): {true_duplicates_check.count()}")
"""

# COMMAND ----------

# Duplicates Dropped
base_df = base_df.dropDuplicates()

# COMMAND ----------

# Replacing all the null values with 'Unknown'
base_df = base_df.fillna({"vin": "Unknown", "option_desc": "Unknown", "model_text": "Unknown"})
options_df = options_df.fillna({"option_desc": "Unknown"})
vehicle_df = vehicle_df.fillna({"platform": "Unknown", "nameplate_display": "Unknown"})

# Doing some basic cleaning and deriving moedel and model_name columns
base_df = (
    base_df
    .withColumn("model", func.expr("substring(model_text, 1, 4)"))
    .withColumn("model", func.when(func.col("model").isin("Free", "    "), "Unknown").otherwise(func.col("model")))
    .withColumn("model_name", func.expr("substring(model_text, 6, greatest(length(model_text)-5, 0))"))
    .withColumn("model_name",
                func.when((func.col("model_name") == "") | (func.col("model_name") == "ander /LR2"), "Unknown")
                  .otherwise(func.col("model_name")))
)

# Deriving net_quantity and net_sales_price columns
agg_base_df = (
    base_df.groupBy('vin', 'option_code', 'option_desc', 'model_name', 'model').agg(
        func.sum('option_quantities').alias('net_quantity'),
        func.sum('sales_price').alias('net_sales_price')
        ).withColumn(
            'net_quantity',
            func.col('net_quantity').cast(t.DoubleType())
            ).withColumn(
                'net_sales_price',
                func.col('net_sales_price').cast(t.DoubleType())
                )
    )

#options_df.show()

# COMMAND ----------

# Checking and emptying the parquet folders.
def folder_exists(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False

parquet_folder_paths = [OUT_BASE, OUT_OPTS, OUT_VLM, OUT_BASE_PARQUET, OUT_OPTS_PARQUET, OUT_VLM_PARQUET]
for parquet_folder_path in parquet_folder_paths:
    if folder_exists(parquet_folder_path):
        print(f"Folder exists with data: {parquet_folder_path}")
        dbutils.fs.rm(parquet_folder_path, recurse=True)
        print("Deleted old data")
    else:
        print(f"Folder is clean or does not exist: {parquet_folder_path}")

# COMMAND ----------

# Loading the data to parquet files
(
    agg_base_df
    .withColumn("run_date", func.lit(RUN_DATE))
    .write.mode("overwrite").partitionBy("run_date")
    .parquet(OUT_BASE)
)

(
    options_df
    .withColumn("run_date", func.lit(RUN_DATE))
    .write.mode("overwrite").partitionBy("run_date")
    .parquet(OUT_OPTS)
)

(
    vehicle_df
    .withColumn("run_date", func.lit(RUN_DATE))
    .write.mode("overwrite").partitionBy("run_date")
    .parquet(OUT_VLM)
)



# COMMAND ----------

agg_base_df.count()

# COMMAND ----------

exit()

# COMMAND ----------

