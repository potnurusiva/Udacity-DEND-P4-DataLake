Purpose: Sparkify a new startup company and it's analytics team wanted to perform some amalysis on their music streaming app to know the songs users are listening to. This project contains the Sparkity data modelling with S3 buckets to parquet files and the entire ETL process to extract, process and load the data into parquet files which have final analytical tables data. This will help users from Sparkify to query the data based on their business requirements and identify the areas to improve their music streaming app.

Project summary: Create the sparkify data in structured way and store it in S3 buckets as parquet files, so that Sparkify team can query the data from different tables based on their business needs 


Input data : Resides in JSON log and song files which are stored in S3 buckets
Output data: Parquet files stored in amazon S3 buckets

ETL Schema : Star

Parquet files: songs - contains song information including song_id, title, artist_id, year, duration
        artists - contains artist information including artist_id, name, location, lattitude, longitude
        users - contains users information including user_id, first_name, last_name, gender, level
        time - contains song play time information including date and time values
        songplays - Fact table -consists most of the information combined form song and log data files
       
Files:
1. etl.py - contains the code to Extract the data from json files which are in S3 buckets, process it and load into the parquet  files
2. dl.cfg - Fill all your AWS S3 configuration details here before creating and loading the parquet files

Execution guidelines:
1. Fill your aws access key and aws secret access key in dl.cfg file before etl.py execution
2. Execute etl.py after succesful completion of the first step for loading the data from JSON files into parquet files in s3 buckets
        