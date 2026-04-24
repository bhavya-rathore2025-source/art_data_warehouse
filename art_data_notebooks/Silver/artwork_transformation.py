# Databricks notebook source
artworks_df= spark.table('dev.bronze.artworks')
artworks_df.limit(100).display()

# COMMAND ----------

from pyspark.sql.functions import expr,regexp_extract,nullif,year,col

artworks_df= artworks_df.where(expr('object_id is not null'))

a2= (
    artworks_df.selectExpr('cast(object_id as int) as object_id',
                           'constituent_id',
                           'trim(title) as title',
                           'try_cast(left(date,4) as int) as artwork_year',
                            'try_cast(date_acquired as date) as date_acquired',
                            'lower(trim(medium)) as medium',
                            'trim(classification) as classification',
                            'trim(department) as department',
                            'trim(credit_line) as credit_line_d',
                            'dimensions',
                            'artist',
                            'artist_bio',
                            'nationality',
                            'gender',
                            'begin_date',
                            'end_date',
                            'url as url',
                            'image_url'
                           )
)

#extracting proper dimensions from mesyy dimension values
a2 = a2.withColumn("dim1_cm", regexp_extract("dimensions", r"(\d+\.?\d*)", 1)) \
       .withColumn("dim2_cm", regexp_extract("dimensions", r"x\s*(\d+\.?\d*)", 1))


a2 = (
    a2.withColumn("dim1_cm", expr('cast(nullif(dim1_cm, "") as double)'))
       .withColumn("dim2_cm", expr('cast(nullif(dim2_cm, "") as double)')) 
       .filter(
    col("artwork_year") > 
    regexp_extract(col("begin_date"), "\\d+", 0).cast("int"))
)

a2.write.mode('overwrite').saveAsTable('dev.silver.artworks')
a2.display()