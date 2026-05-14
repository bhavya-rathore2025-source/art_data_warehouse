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

artist_path='/Volumes/dev/bronze/datasets/Artists.csv'
artwork_path= '/Volumes/dev/bronze/datasets/Artworks.csv'