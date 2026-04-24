# Databricks notebook source
artworks_df= spark.table('dev.bronze.artworks')

artworks_df.limit(30).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### checking nulls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_count,
# MAGIC
# MAGIC     COUNT(CASE WHEN title IS NULL THEN 1 END) AS title_nulls,
# MAGIC     COUNT(CASE WHEN artist IS NULL THEN 1 END) AS artist_nulls,
# MAGIC     COUNT(CASE WHEN constituent_id IS NULL THEN 1 END) AS constituent_id_nulls,
# MAGIC     COUNT(CASE WHEN artist_bio IS NULL THEN 1 END) AS artist_bio_nulls,
# MAGIC     COUNT(CASE WHEN nationality IS NULL THEN 1 END) AS nationality_nulls,
# MAGIC     COUNT(CASE WHEN begin_date IS NULL THEN 1 END) AS begin_date_nulls,
# MAGIC     COUNT(CASE WHEN end_date IS NULL THEN 1 END) AS end_date_nulls,
# MAGIC     COUNT(CASE WHEN gender IS NULL THEN 1 END) AS gender_nulls,
# MAGIC     COUNT(CASE WHEN date IS NULL THEN 1 END) AS date_nulls,
# MAGIC     COUNT(CASE WHEN medium IS NULL THEN 1 END) AS medium_nulls,
# MAGIC     COUNT(CASE WHEN dimensions IS NULL THEN 1 END) AS dimensions_nulls,
# MAGIC     COUNT(CASE WHEN credit_line IS NULL THEN 1 END) AS credit_line_nulls,
# MAGIC     COUNT(CASE WHEN accession_number IS NULL THEN 1 END) AS accession_number_nulls,
# MAGIC     COUNT(CASE WHEN classification IS NULL THEN 1 END) AS classification_nulls,
# MAGIC     COUNT(CASE WHEN department IS NULL THEN 1 END) AS department_nulls,
# MAGIC     COUNT(CASE WHEN date_acquired IS NULL THEN 1 END) AS date_acquired_nulls,
# MAGIC     COUNT(CASE WHEN cataloged IS NULL THEN 1 END) AS cataloged_nulls,
# MAGIC     COUNT(CASE WHEN object_id IS NULL THEN 1 END) AS object_id_nulls,
# MAGIC     COUNT(CASE WHEN url IS NULL THEN 1 END) AS url_nulls,
# MAGIC     COUNT(CASE WHEN image_url IS NULL THEN 1 END) AS image_url_nulls,
# MAGIC     COUNT(CASE WHEN on_view IS NULL THEN 1 END) AS on_view_nulls,
# MAGIC
# MAGIC     COUNT(CASE WHEN circumference_cm IS NULL THEN 1 END) AS circumference_nulls,
# MAGIC     COUNT(CASE WHEN depth_cm IS NULL THEN 1 END) AS depth_nulls,
# MAGIC     COUNT(CASE WHEN diameter_cm IS NULL THEN 1 END) AS diameter_nulls,
# MAGIC     COUNT(CASE WHEN height_cm IS NULL THEN 1 END) AS height_nulls,
# MAGIC     COUNT(CASE WHEN length_cm IS NULL THEN 1 END) AS length_nulls,
# MAGIC     COUNT(CASE WHEN weight_kg IS NULL THEN 1 END) AS weight_nulls,
# MAGIC     COUNT(CASE WHEN width_cm IS NULL THEN 1 END) AS width_nulls,
# MAGIC     COUNT(CASE WHEN seat_height_cm IS NULL THEN 1 END) AS seat_height_nulls,
# MAGIC     COUNT(CASE WHEN duration_sec IS NULL THEN 1 END) AS duration_nulls
# MAGIC
# MAGIC FROM dev.bronze.artworks;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checking empty strings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN TRIM(title) = '' THEN 1 END) AS title_empty,
# MAGIC     COUNT(CASE WHEN TRIM(artist) = '' THEN 1 END) AS artist_empty,
# MAGIC     COUNT(CASE WHEN TRIM(constituent_id) = '' THEN 1 END) AS constituent_id_empty,
# MAGIC     COUNT(CASE WHEN TRIM(artist_bio) = '' THEN 1 END) AS artist_bio_empty,
# MAGIC     COUNT(CASE WHEN TRIM(nationality) = '' THEN 1 END) AS nationality_empty,
# MAGIC     COUNT(CASE WHEN TRIM(begin_date) = '' THEN 1 END) AS begin_date_empty,
# MAGIC     COUNT(CASE WHEN TRIM(end_date) = '' THEN 1 END) AS end_date_empty,
# MAGIC     COUNT(CASE WHEN TRIM(gender) = '' THEN 1 END) AS gender_empty,
# MAGIC     COUNT(CASE WHEN TRIM(date) = '' THEN 1 END) AS date_empty,
# MAGIC     COUNT(CASE WHEN TRIM(medium) = '' THEN 1 END) AS medium_empty,
# MAGIC     COUNT(CASE WHEN TRIM(dimensions) = '' THEN 1 END) AS dimensions_empty,
# MAGIC     COUNT(CASE WHEN TRIM(credit_line) = '' THEN 1 END) AS credit_line_empty,
# MAGIC     COUNT(CASE WHEN TRIM(accession_number) = '' THEN 1 END) AS accession_number_empty,
# MAGIC     COUNT(CASE WHEN TRIM(classification) = '' THEN 1 END) AS classification_empty,
# MAGIC     COUNT(CASE WHEN TRIM(department) = '' THEN 1 END) AS department_empty,
# MAGIC     COUNT(CASE WHEN TRIM(date_acquired) = '' THEN 1 END) AS date_acquired_empty,
# MAGIC     COUNT(CASE WHEN TRIM(cataloged) = '' THEN 1 END) AS cataloged_empty,
# MAGIC     COUNT(CASE WHEN TRIM(object_id) = '' THEN 1 END) AS object_id_empty,
# MAGIC     COUNT(CASE WHEN TRIM(url) = '' THEN 1 END) AS url_empty,
# MAGIC     COUNT(CASE WHEN TRIM(image_url) = '' THEN 1 END) AS image_url_empty,
# MAGIC     COUNT(CASE WHEN TRIM(on_view) = '' THEN 1 END) AS on_view_empty,
# MAGIC     COUNT(CASE WHEN TRIM(circumference_cm) = '' THEN 1 END) AS circumference_empty,
# MAGIC     COUNT(CASE WHEN TRIM(depth_cm) = '' THEN 1 END) AS depth_empty,
# MAGIC     COUNT(CASE WHEN TRIM(diameter_cm) = '' THEN 1 END) AS diameter_empty,
# MAGIC     COUNT(CASE WHEN TRIM(height_cm) = '' THEN 1 END) AS height_empty,
# MAGIC     COUNT(CASE WHEN TRIM(length_cm) = '' THEN 1 END) AS length_empty,
# MAGIC     COUNT(CASE WHEN TRIM(weight_kg) = '' THEN 1 END) AS weight_empty,
# MAGIC     COUNT(CASE WHEN TRIM(width_cm) = '' THEN 1 END) AS width_empty,
# MAGIC     COUNT(CASE WHEN TRIM(seat_height_cm) = '' THEN 1 END) AS seat_height_empty,
# MAGIC     COUNT(CASE WHEN TRIM(duration_sec) = '' THEN 1 END) AS duration_empty
# MAGIC
# MAGIC FROM dev.bronze.artworks;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Checking whitespaces

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(CASE WHEN title != TRIM(title) THEN 1 END) AS title_ws,
# MAGIC     COUNT(CASE WHEN artist != TRIM(artist) THEN 1 END) AS artist_ws,
# MAGIC     COUNT(CASE WHEN constituent_id != TRIM(constituent_id) THEN 1 END) AS constituent_id_ws,
# MAGIC     COUNT(CASE WHEN artist_bio != TRIM(artist_bio) THEN 1 END) AS artist_bio_ws,
# MAGIC     COUNT(CASE WHEN nationality != TRIM(nationality) THEN 1 END) AS nationality_ws,
# MAGIC     COUNT(CASE WHEN begin_date != TRIM(begin_date) THEN 1 END) AS begin_date_ws,
# MAGIC     COUNT(CASE WHEN end_date != TRIM(end_date) THEN 1 END) AS end_date_ws,
# MAGIC     COUNT(CASE WHEN gender != TRIM(gender) THEN 1 END) AS gender_ws,
# MAGIC     COUNT(CASE WHEN date != TRIM(date) THEN 1 END) AS date_ws,
# MAGIC     COUNT(CASE WHEN medium != TRIM(medium) THEN 1 END) AS medium_ws,
# MAGIC     COUNT(CASE WHEN dimensions != TRIM(dimensions) THEN 1 END) AS dimensions_ws,
# MAGIC     COUNT(CASE WHEN credit_line != TRIM(credit_line) THEN 1 END) AS credit_line_ws,
# MAGIC     COUNT(CASE WHEN accession_number != TRIM(accession_number) THEN 1 END) AS accession_number_ws,
# MAGIC     COUNT(CASE WHEN classification != TRIM(classification) THEN 1 END) AS classification_ws,
# MAGIC     COUNT(CASE WHEN department != TRIM(department) THEN 1 END) AS department_ws,
# MAGIC     COUNT(CASE WHEN date_acquired != TRIM(date_acquired) THEN 1 END) AS date_acquired_ws,
# MAGIC     COUNT(CASE WHEN cataloged != TRIM(cataloged) THEN 1 END) AS cataloged_ws,
# MAGIC     COUNT(CASE WHEN object_id != TRIM(object_id) THEN 1 END) AS object_id_ws,
# MAGIC     COUNT(CASE WHEN url != TRIM(url) THEN 1 END) AS url_ws,
# MAGIC     COUNT(CASE WHEN image_url != TRIM(image_url) THEN 1 END) AS image_url_ws,
# MAGIC     COUNT(CASE WHEN on_view != TRIM(on_view) THEN 1 END) AS on_view_ws,
# MAGIC     COUNT(CASE WHEN circumference_cm != TRIM(circumference_cm) THEN 1 END) AS circumference_ws,
# MAGIC     COUNT(CASE WHEN depth_cm != TRIM(depth_cm) THEN 1 END) AS depth_ws,
# MAGIC     COUNT(CASE WHEN diameter_cm != TRIM(diameter_cm) THEN 1 END) AS diameter_ws,
# MAGIC     COUNT(CASE WHEN height_cm != TRIM(height_cm) THEN 1 END) AS height_ws,
# MAGIC     COUNT(CASE WHEN length_cm != TRIM(length_cm) THEN 1 END) AS length_ws,
# MAGIC     COUNT(CASE WHEN weight_kg != TRIM(weight_kg) THEN 1 END) AS weight_ws,
# MAGIC     COUNT(CASE WHEN width_cm != TRIM(width_cm) THEN 1 END) AS width_ws,
# MAGIC     COUNT(CASE WHEN seat_height_cm != TRIM(seat_height_cm) THEN 1 END) AS seat_height_ws,
# MAGIC     COUNT(CASE WHEN duration_sec != TRIM(duration_sec) THEN 1 END) AS duration_ws
# MAGIC
# MAGIC FROM dev.bronze.artworks;

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