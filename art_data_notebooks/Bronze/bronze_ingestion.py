#git testing


def load_artist_bronze(spark,artist_schema,artist_path):
    artist_df= (
    spark.read.format('csv')
    .option('header','true')
    .schema(artist_schema)
    .load(artist_path)
    )
    return artist_df

def load_artwork_bronze(spark,artwork_schema,artwork_path):
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
    .load(artwork_path)
    )
    return artwork_df





