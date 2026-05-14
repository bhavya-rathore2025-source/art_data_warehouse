# Databricks notebook source
artworks_df= spark.table('dev.bronze.artworks')

artworks_df.limit(30).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Quality Check

# COMMAND ----------

from data_quality import data_quality_summary
result_df= data_quality_summary(artworks_df)

display(result_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Constituent_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check for duplicates values
# MAGIC SELECT COUNT(*) as total_rows , COUNT(DISTINCT object_id) as total_unique_id
# MAGIC FROM dev.bronze.artworks;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC --Find number of dimension suitable for transformation
# MAGIC SELECT COUNT(*) AS valid_with_single
# MAGIC FROM dev.bronze.artworks
# MAGIC WHERE dimensions RLIKE '\\(([0-9\\.]+\\s*[xX×]\\s*){1,2}[0-9\\.]+\\s*cm\\)'
# MAGIC    OR dimensions RLIKE '\\([0-9\\.]+\\s*cm\\)';

# COMMAND ----------

# MAGIC %md
# MAGIC #####Conclusion for artwork table
# MAGIC
# MAGIC 1.Many columns will have to be dropped since they are either duplicates, or mostly nulls
# MAGIC
# MAGIC 2.Heavy transformation is required to extract useful data 
