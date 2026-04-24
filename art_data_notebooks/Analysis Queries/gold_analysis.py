# Databricks notebook source
# MAGIC %md
# MAGIC ####1. Join test

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     f.object_id,
# MAGIC     f.title,
# MAGIC     d.display_name
# MAGIC FROM dev.gold.artworks f
# MAGIC JOIN dev.gold.bridge_artwork_artist b 
# MAGIC     ON f.object_id = b.object_id
# MAGIC JOIN dev.gold.artists d 
# MAGIC     ON b.artist_id = d.artist_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Top artists by artworks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.display_name,
# MAGIC     COUNT(*) AS artwork_count
# MAGIC FROM dev.gold.bridge_artwork_artist b
# MAGIC JOIN dev.gold.artists d 
# MAGIC     ON b.artist_id = d.artist_id
# MAGIC GROUP BY d.display_name
# MAGIC ORDER BY artwork_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ####3. Artworks per year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     artwork_year,
# MAGIC     COUNT(*) AS total_artworks
# MAGIC FROM dev.gold.artworks
# MAGIC GROUP BY artwork_year
# MAGIC ORDER BY artwork_year

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Most common medium

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     medium,
# MAGIC     COUNT(*) AS count
# MAGIC FROM dev.gold.artworks
# MAGIC GROUP BY medium
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Average dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     round(AVG(dim1_cm),2) AS avg_dim1,
# MAGIC     round(AVG(dim2_cm),2) AS avg_dim2
# MAGIC FROM dev.gold.artworks;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC