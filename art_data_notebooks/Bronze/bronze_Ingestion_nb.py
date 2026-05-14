# Databricks notebook source
# MAGIC %md
# MAGIC 01- Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev;
# MAGIC CREATE DATABASE IF NOT EXISTS dev.bronze;
# MAGIC CREATE DATABASE IF NOT EXISTS dev.silver;
# MAGIC CREATE DATABASE IF NOT EXISTS dev.gold;
# MAGIC CREATE VOLUME IF NOT EXISTS dev.bronze.datasets

# COMMAND ----------

# MAGIC %md
# MAGIC 02-Loading artist and artworks 

# COMMAND ----------

from bronze_ingestion import load_artist_bronze,load_artwork_bronze
from schema_paths import artist_schema,artist_path,artwork_schema,artwork_path

# COMMAND ----------

artist_df= load_artist_bronze(spark,artist_schema,artist_path)
artwork_df = load_artwork_bronze(spark,artwork_schema,artwork_path)

artwork_df.write.mode('overwrite').saveAsTable('dev.bronze.artworks')
artist_df.write.mode('overwrite').saveAsTable('dev.bronze.artists')
