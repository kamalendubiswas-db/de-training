# Databricks notebook source
# MAGIC %md
# MAGIC # This notebook is used to:
# MAGIC - #### Copy pulse data to Unity Catalog Volume
# MAGIC - #### Create Bronze layer table
# MAGIC - #### Transform the pulse data
# MAGIC - #### Create Silver layer table

# COMMAND ----------


# DBTITLE 1,Setup Variables
# input your details
username = f"odl_instructor_1280678@databrickslabs.com"
user_prefix = f"kb"

# Setup all required paths
source_repo_path = f"/Workspace/Repos/{username}/de-training/DE Training Pulse Check (Responses) - Form Responses 1.csv"
my_catalog = f"{user_prefix}_utrecht_training"
my_volume = f"pulse_check"
target_file_path = f"/Volumes/{my_catalog}/bronze/{my_volume}/pulse_data.csv"

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema and Volume
catalog_sql = f"CREATE CATALOG IF NOT EXISTS {my_catalog}"
spark.sql(catalog_sql)

schema_list = ['bronze', 'silver', 'gold']
for schema in schema_list:
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {my_catalog}.{schema}"
    spark.sql(schema_sql)

volume_sql = f"CREATE VOLUME IF NOT EXISTS {my_catalog}.bronze.{my_volume}"
spark.sql(volume_sql)

# COMMAND ----------

# DBTITLE 1,Copy raw pulse csv to UC Volume
import os
import shutil

# Move the source file to the target path
shutil.copy(source_repo_path, target_file_path)

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Provide the schema
schema = StructType([
    StructField("created_at", StringType(), True),
    StructField("job_role", StringType(), True),
    StructField("training_type", StringType(), True),
    StructField("databricks_knowledge", StringType(), True),
    StructField("favorite_city", StringType(), True),
    # Add as many fields as you have in your CSV
])

# Read the pulse data csv
bronze_df = spark.read.options(multiline = True, delimiter=",", header=True).schema(schema).csv(target_file_path)

# Save as bronze table
bronze_df.write.mode("overwrite").saveAsTable(f"{my_catalog}.bronze.raw_pulse_data")

# COMMAND ----------

# DBTITLE 1,Create Silver Table
from pyspark.sql.functions import split

silver_df = spark.read.table(f"{my_catalog}.bronze.raw_pulse_data")

# Split the 'favorite_city' column into two parts: 'city' and 'rating'
# The split function uses a regex pattern that looks for a hyphen possibly surrounded by spaces
split_col = split(silver_df['favorite_city'], ' - | -|-|- ')

# Add the split columns to the DataFrame
silver_df = silver_df.withColumn('city', split_col.getItem(0))
silver_df = silver_df.withColumn('rating', split_col.getItem(1))

# The 'rating' column is currently of type string, convert it to integer
transfomed_df = silver_df.withColumn("rating", silver_df["rating"].cast(IntegerType()))

# Save as silver table
transfomed_df.write.mode("overwrite").saveAsTable(f"{my_catalog}.silver.transformed_pulse_data")
