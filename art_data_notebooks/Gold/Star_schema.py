# Databricks notebook source
artwork_df= spark.table('dev.silver.artworks')

artwork_df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Bridge table

# COMMAND ----------

from pyspark.sql.functions import split, explode, col,expr

bridge = artwork_df.select(
    "object_id",
    explode(split(col("constituent_id"), ",")).alias("artist_id")
)
bridge = bridge.withColumn("artist_id", expr("try_cast(trim(artist_id) as int)")).dropna()

bridge.write.mode('overwrite').saveAsTable('dev.gold.bridge_artwork_artist')
bridge.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Artworks table

# COMMAND ----------

artworks_df= spark.table('dev.spark_db.silver_artworks')

fact_artwork = artwork_df.selectExpr(
    "object_id",
    "title",
    "artwork_year",
    "date_acquired",
    "medium",
    "classification",
    "department",
    "credit_line_d",
    "dim1_cm",
    "dim2_cm"
).dropDuplicates(['object_id'])

fact_artwork.write.mode('overwrite').saveAsTable('dev.gold.artworks')

fact_artwork.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Artist table

# COMMAND ----------

artist_df= spark.table('dev.silver.artists')

from pyspark.sql.functions import col

dim_artist = artist_df.select(
    col("constituent_id").alias("artist_id"),
    "display_name",
    "artist_bio",
    "nationality",
    "gender",
    "begin_date",
    "end_date"
).dropDuplicates(['artist_id'])

dim_artist.write.mode('overwrite').saveAsTable('dev.gold.artists')
