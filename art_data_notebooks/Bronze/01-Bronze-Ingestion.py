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

artist_schema = '''
constituent_id string,
display_name string,
artist_bio string,
nationality string,
gender string,
begin_date string,
end_date string,
wiki_qid string,
ulan string
'''

artwork_schema = '''
title string,
artist string,
constituent_id string,
artist_bio string,
nationality string,
begin_date string,
end_date string,
gender string,
date string,
medium string,
dimensions string,
credit_line string,
accession_number string,
classification string,
department string,
date_acquired string,
cataloged string,
object_id string,
url string,
image_url string,
on_view string,
circumference_cm string,
depth_cm string,
diameter_cm string,
height_cm string,
length_cm string,
weight_kg string,
width_cm string,
seat_height_cm string,
duration_sec string
'''

# COMMAND ----------

artist_df= (
    spark.read.format('csv')
    .option('header','true')
    .schema(artist_schema)
    .load('/Volumes/dev/bronze/datasets/Artists.csv')
)
artwork_df = (
    spark.read.format('csv')
    .option('header', 'true')
    .option('multiLine', 'true')
    .option('quote', '"')
    .option('escape', '"')
    .option('mode', 'PERMISSIVE')
    .option('inferSchema', 'false')
    .option('columnNameOfCorruptRecord', '_corrupt_record')
    .schema(artwork_schema)
    .load('/Volumes/dev/bronze/datasets/Artworks.csv')
)

artwork_df.write.mode('overwrite').saveAsTable('dev.bronze.artworks')
artist_df.write.mode('overwrite').saveAsTable('dev.bronze.artists')