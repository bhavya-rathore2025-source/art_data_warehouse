# Databricks notebook source
artists_df= spark.table('dev.bronze.artists')

artists_df.limit(30).display()

# COMMAND ----------

from pyspark.sql.functions import replace
# Data standardization
gender_expr="""
    case 
        when LOWER(gender)='male' then 'm'
        when Lower(gender)='female' then 'f' 
        when gender is null then null
        else 'other'
    end as gender
"""


final_artist_df= (
    artists_df
        .selectExpr('try_cast(constituent_id as int) as constituent_id',
                        'trim(display_name) as display_name',
                        'artist_bio',
                        'trim(nationality) as nationality',
                        gender_expr,
                        'cast(begin_date as int) as begin_date',
                        'cast(end_date as int) as end_date',
                        'wiki_qid',
                        'ulan'
                    )
        .filter('constituent_id is not null')
        .replace(0,None,subset=["begin_date","end_date"]) #replacing 0 with nulls
    
)

final_artist_df.write.mode('overwrite').saveAsTable('dev.silver.artists')
final_artist_df.display()