# Databricks notebook source
# MAGIC %md
# MAGIC ## This notebook is used to transform the pulse data from Repo, copy it to Unity Catalog Volume and Tranform it.

# COMMAND ----------

# DBTITLE 1,Download Pulse Data from Git
import os
import shutil

source_repo_path = "/Workspace/Repos/odl_instructor_1280678@databrickslabs.com/de-training/DE Training Pulse Check (Responses) - Form Responses 1.csv"

target_volume_path =  "/Volumes/kb_utrecht_training/bronze/pulse_check/pulse_data.csv"

# Move the source file to the target path
shutil.copy(source_repo_path, target_volume_path)

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
bronze_df = spark.read.options(multiline = True, delimiter=",", header=True).schema(schema).csv(target_volume_path)

# Save as bronze table
bronze_df.write.mode("overwrite").saveAsTable("kb_utrecht_training.bronze.raw_pulse_data")

# COMMAND ----------

# DBTITLE 1,Create Silver Table
from pyspark.sql.functions import split

silver_df = spark.read.table("kb_utrecht_training.bronze.raw_pulse_data")

# Split the 'favorite_city' column into two parts: 'city' and 'rating'
# The split function uses a regex pattern that looks for a hyphen possibly surrounded by spaces
split_col = split(silver_df['favorite_city'], ' - | -|-|- ')

# Add the split columns to the DataFrame
silver_df = silver_df.withColumn('city', split_col.getItem(0))
silver_df = silver_df.withColumn('rating', split_col.getItem(1))

# The 'rating' column is currently of type string, convert it to integer
transfomed_df = silver_df.withColumn("rating", silver_df["rating"].cast(IntegerType()))

# Save as silver table
transfomed_df.write.mode("overwrite").saveAsTable("kb_utrecht_training.silver.transformed_pulse_data")
