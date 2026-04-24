# Databricks notebook source
artists_df= spark.table('dev.bronze.artists')

artists_df.limit(30).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking nulls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) as total_count,
# MAGIC     COUNT(CASE WHEN constituent_id IS NULL THEN 1 END) AS id_nulls,
# MAGIC     COUNT(CASE WHEN display_name IS NULL THEN 1 END) AS name_nulls,
# MAGIC     COUNT(CASE WHEN artist_bio IS NULL THEN 1 END) AS bio_nulls,
# MAGIC     COUNT(CASE WHEN nationality IS NULL THEN 1 END) AS nationality_nulls,
# MAGIC     COUNT(CASE WHEN gender IS NULL THEN 1 END) AS gender_nulls,
# MAGIC     COUNT(CASE WHEN begin_date IS NULL THEN 1 END) AS birth_nulls,
# MAGIC     COUNT(CASE WHEN end_date IS NULL THEN 1 END) AS end_nulls,
# MAGIC     COUNT(CASE WHEN wiki_qid IS NULL THEN 1 END) AS wiki_id_nulls,
# MAGIC     COUNT(CASE WHEN ulan IS NULL THEN 1 END) AS ulan_nulls
# MAGIC FROM dev.bronze.artists;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checking Empty Strings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN TRIM(constituent_id) = '' THEN 1 END) AS constituent_id_empty,
# MAGIC     COUNT(CASE WHEN TRIM(display_name) = '' THEN 1 END) AS display_name_empty,
# MAGIC     COUNT(CASE WHEN TRIM(artist_bio) = '' THEN 1 END) AS artist_bio_empty,
# MAGIC     COUNT(CASE WHEN TRIM(nationality) = '' THEN 1 END) AS nationality_empty,
# MAGIC     COUNT(CASE WHEN TRIM(gender) = '' THEN 1 END) AS gender_empty,
# MAGIC     COUNT(CASE WHEN TRIM(begin_date) = '' THEN 1 END) AS begin_date_empty,
# MAGIC     COUNT(CASE WHEN TRIM(end_date) = '' THEN 1 END) AS end_date_empty,
# MAGIC     COUNT(CASE WHEN TRIM(wiki_qid) = '' THEN 1 END) AS wiki_qid_empty,
# MAGIC     COUNT(CASE WHEN TRIM(ulan) = '' THEN 1 END) AS ulan_empty
# MAGIC FROM dev.bronze.artists;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checking Whitespaces

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN constituent_id != TRIM(constituent_id) THEN 1 END) AS constituent_id_ws,
# MAGIC     COUNT(CASE WHEN display_name != TRIM(display_name) THEN 1 END) AS display_name_ws,
# MAGIC     COUNT(CASE WHEN artist_bio != TRIM(artist_bio) THEN 1 END) AS artist_bio_ws,
# MAGIC     COUNT(CASE WHEN nationality != TRIM(nationality) THEN 1 END) AS nationality_ws,
# MAGIC     COUNT(CASE WHEN gender != TRIM(gender) THEN 1 END) AS gender_ws,
# MAGIC     COUNT(CASE WHEN begin_date != TRIM(begin_date) THEN 1 END) AS begin_date_ws,
# MAGIC     COUNT(CASE WHEN end_date != TRIM(end_date) THEN 1 END) AS end_date_ws,
# MAGIC     COUNT(CASE WHEN wiki_qid != TRIM(wiki_qid) THEN 1 END) AS wiki_qid_ws,
# MAGIC     COUNT(CASE WHEN ulan != TRIM(ulan) THEN 1 END) AS ulan_ws
# MAGIC FROM dev.bronze.artists;

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