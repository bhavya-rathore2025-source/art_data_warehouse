# Databricks notebook source
artists_df= spark.table('dev.bronze.artists')

artists_df.limit(30).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Quality Check

# COMMAND ----------

from data_quality import data_quality_summary

result_df = data_quality_summary(artists_df)

display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### constituent_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for duplicates values
# MAGIC select count(distinct(constituent_id)) as distinct_count,count(constituent_id) as normal_count
# MAGIC from dev.bronze.artists;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Checking for unusual id
# MAGIC SELECT *
# MAGIC FROM dev.bronze.artists
# MAGIC WHERE constituent_id LIKE '%,%';

# COMMAND ----------

# MAGIC %md
# MAGIC #####gender

# COMMAND ----------

# MAGIC %sql
# MAGIC --Checking unusual genders
# MAGIC select gender from dev.bronze.artists
# MAGIC where gender != 'male' and gender!= 'female' and gender is not null; 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dates

# COMMAND ----------

# MAGIC %sql
# MAGIC --Finding any unrealistic dates, 0 will be converted to null
# MAGIC select min(begin_date),min(end_date),max(begin_date),max(end_date) from dev.bronze.artists;

# COMMAND ----------

# MAGIC %sql
# MAGIC --quick check on dates to find unusual values
# MAGIC SELECT DISTINCT begin_date
# MAGIC FROM dev.bronze.artists
# MAGIC ORDER BY cast(begin_date as int);

# COMMAND ----------

# MAGIC %sql
# MAGIC --checking data of the only artist from 1181,rest are 1700+
# MAGIC select * from dev.bronze.artists
# MAGIC where begin_date= '1181';

# COMMAND ----------

# MAGIC %sql
# MAGIC --quick check on dates to find unusual values
# MAGIC SELECT DISTINCT end_date
# MAGIC FROM dev.bronze.artists
# MAGIC ORDER BY cast(end_date as int);

# COMMAND ----------

# MAGIC %sql
# MAGIC --Finding dates with - or ( at start
# MAGIC select begin_date,end_date
# MAGIC from dev.bronze.artists
# MAGIC where (left(begin_date,1) in ('-','(')) or (left(end_date,1) in ('-','('))

# COMMAND ----------

# MAGIC %sql
# MAGIC --Finding any date with alphabet
# MAGIC SELECT begin_date,end_date FROM dev.bronze.artists
# MAGIC WHERE TRY_CAST(begin_date AS INT) IS NULL or try_cast(end_date as int) is null

# COMMAND ----------

# MAGIC %md
# MAGIC ####Conclusion for artist
# MAGIC
# MAGIC 1. Wiki and ulan are unusable and will be dropped in gold table
# MAGIC 2. Set males to M and females to f to,others to other
# MAGIC 3. Change date of 0 value to null
# MAGIC 4. Calculate age wherever possible
