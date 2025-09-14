# Databricks notebook source
# Importing necessry libraries and creating a spark session.
from pyspark.sql import SparkSession, functions as func, types as t, Window
import datetime as dt
from pyspark.sql import functions as func, types as t

spark = (SparkSession.builder
         .appName("step7-data_enrichement")
         .getOrCreate())

# COMMAND ----------

# Setting up some configuration variables.
PROJECT = "enrichment-etl-jlr"
TRANS_DS = "transformed"
RUN_DATE = dt.date.today().strftime("%Y%m%d")

# Loading the data.
enriched_df = (spark.read.format("bigquery")
        .option("table", f"{PROJECT}.{TRANS_DS}.enriched_data")
        .load())


# COMMAND ----------

# Preparing data
enriched_df = enriched_df.filter(func.col('net_sales_price').cast('double').isNotNull())
enriched_df = enriched_df.filter(func.col('production_cost').cast('double').isNotNull())
# Added a new column profit_per_transaction with Nulls
enriched_df = enriched_df.withColumn('profit_per_transaction', func.lit(None).cast('double'))

# COMMAND ----------

#enriched_df.show()

# COMMAND ----------

# Null Value checks on enriched_df
"""null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)"""

# COMMAND ----------

# Computing profit per transaction.
enriched_df = enriched_df.withColumn(
    'profit_per_transaction',
    func.col('net_sales_price') - func.col('production_cost')
)

# COMMAND ----------

#enriched_df.show()

# COMMAND ----------

# Null Value checks on enriched_df
"""null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)"""

# COMMAND ----------

# This will show any rows where profit_per_transaction is not a number
#enriched_df.filter(func.col('profit_per_transaction').cast('double').isNull()).show()

# COMMAND ----------

## Write the updated dataset to csv for reporting
enriched_df.coalesce(1).write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("gs://enrichment-etl-jlr/curated/output.csv")

# COMMAND ----------

# Checking and emptying the parquet folders.
def folder_exists(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False

parquet_folder_paths = ["gs://enrichment-etl-jlr/curated/data_parquet/ready.marker",
                        "gs://enrichment-etl-jlr/curated/data/ready.marker"]
for parquet_folder_path in parquet_folder_paths:
    if folder_exists(parquet_folder_path):
        print(f"Folder exists with data: {parquet_folder_path}")
        dbutils.fs.rm(parquet_folder_path, recurse=True)
        print("Deleted old data")
    else:
        print(f"Folder is clean or does not exist: {parquet_folder_path}")

# COMMAND ----------

enriched_df.withColumn(
    "run_date", func.lit(RUN_DATE)
    ).write.mode("overwrite").partitionBy("run_date").parquet('gs://enrichment-etl-jlr/curated/data/')

# COMMAND ----------

exit()

# COMMAND ----------

