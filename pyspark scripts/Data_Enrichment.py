# Databricks notebook source
# Importing necessry libraries and creating a spark session.
from pyspark.sql import SparkSession, functions as func, types as t, Window
import datetime as dt
from pyspark.sql import functions as func, types as t

project  = "enrichment-etl-jlr"
location = "europe-west2"

spark = (SparkSession.builder
         .appName("step7-data_enrichement")
         .getOrCreate())

# COMMAND ----------

# Setting up some configuration variables.
PROJECT_ID = 'enrichment-etl-jlr'
BUCKET = 'enrichment-etl-jlr'
RUN_DATE = dt.date.today().strftime("%Y%m%d")

# COMMAND ----------

# Loading from the BigQuery tables created
base_df = spark.read.format("bigquery").option(
    "table",
    f"{PROJECT_ID}.transformed.base_data").load()
opts_df = spark.read.format("bigquery").option(
    "table",
    f"{PROJECT_ID}.transformed.options_data").load()
vlm_df  = spark.read.format("bigquery").option(
    "table", 
    f"{PROJECT_ID}.transformed.vehicle_line_mapping").load()

# COMMAND ----------

# Added a new column production_cost with Nulls.
enriched_df = base_df.withColumn(
    'production_cost', 
    func.lit(None).cast(t.DecimalType(10, 2))
    )
# Dropping the outliers or negative values.
enriched_df = enriched_df.filter(
    (func.col('net_sales_price').isNotNull())
    )

# COMMAND ----------

#enriched_df.show()

# COMMAND ----------

# Null Value checks on enriched_df
"""
null_report = enriched_df.select(
    [func.sum(
        func.when(
            func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

# Joining the Dataset with options_data
enriched_df = enriched_df.join(
    opts_df.select('model', 'option_code', 'material_cost'),
    (enriched_df.model == opts_df.model) & (enriched_df.option_code == opts_df.option_code),
    how='left_outer'
)

# Dropping the ambigous and unnecessary columns.
enriched_df = enriched_df.drop(opts_df.option_code)
enriched_df = enriched_df.drop(opts_df.model)

# COMMAND ----------

enriched_df.show()

# COMMAND ----------

# Computing average material cost for a particular option_code accross models.
avg_material_cost_by_code = opts_df.filter(
    func.col('material_cost') != 0
).groupBy(
    'option_code'
).agg(
    func.avg('material_cost').alias('avg_material_cost')
)

avg_material_cost_by_code = avg_material_cost_by_code.select(
    func.col("option_code").alias("options_code"),
    "avg_material_cost"
)

# COMMAND ----------



# COMMAND ----------

# Adding the avg_material_cost column to enriched_df
enriched_df = enriched_df.join(
    avg_material_cost_by_code,
    enriched_df.option_code == avg_material_cost_by_code.options_code,
    how = 'left_outer'
)
enriched_df = enriched_df.drop('options_code')


# COMMAND ----------

enriched_df.show()


# COMMAND ----------

# Null value checks on the dataset
"""
null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

# Count of rows that should be updated by rule 1.
count_for_rule_1_test = enriched_df.filter(func.col('net_sales_price') <= 0).count()
print(f"Number of records with net_sales_price <= 0 and to be fixed : {count_for_rule_1_test}")

# COMMAND ----------

# Rule 1: Set 'production_cost' to 0 if 'net_sales_price' is less than or equal to 0, ensuring costs are not assigned to non-revenue-generating items.

enriched_df = enriched_df.withColumn(
    'production_cost',
    func.when(func.col('net_sales_price') <= 0, 0).otherwise(func.col('production_cost'))
)

# COMMAND ----------

# Test after rule 1
count_after_rule_1_test = enriched_df.filter(func.col('production_cost') == 0).count()
print(f"Number of records with net_sales_price <= 0 and are FIXED : {count_after_rule_1_test}")

# COMMAND ----------

# Rule 1 Status
if count_for_rule_1_test != count_after_rule_1_test:
    test_1 = False
    print("Rule 1 failed.")
else:
    test_1 = True
    print("Rule 1 passed.")

# COMMAND ----------

#enriched_df.show()

# COMMAND ----------

# Null value checks on the dataset
"""
null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

count_for_rule_2_test = enriched_df.filter(func.col('production_cost').isNull() &
                                           func.col('material_cost').isNotNull()).count()
print(f"Number of records with production_cost null and material_cost not null : {count_for_rule_2_test}")
count_for_rule_3_test = enriched_df.filter(func.col('production_cost').isNull() &
                                           func.col('material_cost').isNull() &
                                           func.col('avg_material_cost').isNotNull()).count()
print(f"Number of records with production_cost null and material_cost null : {count_for_rule_3_test}")
total_nulls = enriched_df.filter(func.col('production_cost').isNull()).count()
print(f"Total production cost null left in the data : {total_nulls}")
print(f"Total number of records with production_cost null to be fixed in Rule 2 and 3 : {count_for_rule_2_test + count_for_rule_3_test}")
expected_null_after_2_3 = total_nulls - (count_for_rule_2_test + count_for_rule_3_test)
print(f"Remaining production cost null after Rule 2 and 3: {expected_null_after_2_3}")

# COMMAND ----------

# Rule 2: If 'production_cost' is null but 'material_cost' is available, use 'material_cost' as the production cost.
# Rule 3: If both 'production_cost' and 'material_cost' are null, default to the pre-calculated 'avg_material_cost'.
enriched_df = enriched_df.withColumn(
    'production_cost',
    func.when(
        func.col('production_cost').isNull() & func.col('material_cost').isNotNull(),
        func.col('material_cost')
    ).when(
        func.col('production_cost').isNull() & func.col('material_cost').isNull(),
        func.col('avg_material_cost')
    ).otherwise(
        func.col('production_cost')
    )
)

# COMMAND ----------

count_after_rule_2_3_test = enriched_df.filter(func.col('production_cost').isNull()).count()
print(f"Number of records with production_cost null after Rule 2 and 3: {count_after_rule_2_3_test}")

# COMMAND ----------

# Rule 2 and 3 status
if (expected_null_after_2_3) != count_after_rule_2_3_test:
    test_2_3 = False
    print("Rule 2 and 3 failed.")
else:
    test_2_3 = True
    print("Rule 2 and 3 passed.")

# COMMAND ----------

"""
null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

count_for_rule_4_test = enriched_df.filter(func.col('production_cost').isNull()).count()
print(f"Number of records with production_cost null after Rule 4: {count_for_rule_4_test}")

# COMMAND ----------

# Rule 4: If 'production_cost', 'material_cost', and 'avg_material_cost' are all null, estimate the production cost as 45% of the 'net_sales_price'. This provides a fallback calculation when all other cost data is missing.

enriched_df = enriched_df.withColumn(
    'production_cost',
    func.when(
         func.col('production_cost').isNull() &
         func.col('avg_material_cost').isNull() &
         func.col('avg_material_cost').isNull(),
        func.col('net_sales_price') * 0.45
    ).otherwise(
        func.col('production_cost')
    )
)
final_enriched_df = enriched_df.drop('material_cost','avg_material_cost')

# COMMAND ----------

count_after_rule_4_test = enriched_df.filter(func.col('production_cost').isNull()).count()
print(f"Number of records with production_cost null after Rule 4: {count_after_rule_4_test}")
# Rule 4 Status
if count_after_rule_4_test == 0:
    test_4 = True
    print("Rule 4 passed.")
else:
    test_4 = False
    print("Rule 4 failed.")

# COMMAND ----------

# Check if all the rules have been implemented correctly or not
if test_2_3 == True and test_4 == True and test_1 == True:
    print("All the rules have been implemented correctly.")
else:
    print("All the rules have not been implemented correctly.")
    exit()

# COMMAND ----------

"""
null_report = enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

# Joining the enriched data with the vehicle data for reporting

final_enriched_df = final_enriched_df.join(
    vlm_df,
    final_enriched_df.model == vlm_df.nameplate_code,
    how = 'left_outer'
)
final_enriched_df = final_enriched_df.drop(func.col('nameplate_code'))
final_enriched_df = final_enriched_df.drop(vlm_df.run_date)
final_enriched_df = final_enriched_df.na.fill(
    'Unknown',
    subset = ['brand', 'platform', 'nameplate_display']
)

# COMMAND ----------

"""
null_report = final_enriched_df.select(
    [func.sum(func.when(func.col(c).isNull(), 1).otherwise(0)).alias(c) for c in final_enriched_df.columns]
)
print("Null Value Report:")
null_report.show(truncate=False)
"""

# COMMAND ----------

# Checking and emptying the parquet folders.
def folder_exists(path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False

parquet_folder_paths = ["gs://enrichment-etl-jlr/transformed/enriched/", "gs://enrichment-etl-jlr/transformed/enriched_parquet/"]
for parquet_folder_path in parquet_folder_paths:
    if folder_exists(parquet_folder_path):
        print(f"Folder exists with data: {parquet_folder_path}")
        dbutils.fs.rm(parquet_folder_path, recurse=True)
        print("Deleted old data")
    else:
        print(f"Folder is clean or does not exist: {parquet_folder_path}")

# COMMAND ----------

final_enriched_df.show()

# COMMAND ----------

# Writing the enriched data to parquet
final_enriched_df.withColumn(
    "run_date", func.lit(RUN_DATE)
    ).write.mode("overwrite").partitionBy("run_date").parquet('gs://enrichment-etl-jlr/transformed/enriched/')


# COMMAND ----------

exit()