import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime, unix_timestamp, to_date
from pyspark.sql.functions import monotonically_increasing_id
import datetime
import pandas as pd

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(input_data)
    df.dropDuplicates()
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql(""" SELECT DISTINCT(song_id), title, artist_id, year, duration FROM song_data""")
    
    # write songs table to parquet files partitioned by year and artist
    song_data_parquet = output_data + "songs.parquet"
    songs_table.write.partitionBy("year","artist_id").parquet(song_data_parquet, mode="overwrite")
    
    # extract columns to create artists table
    artists_table = spark.sql(""" SELECT DISTINCT(artist_id), artist_name as name, artist_location as location,
                                         artist_latitude as lattitude, artist_longitude as longitude FROM song_data""")
    
    # write artists table to parquet files
    artist_data_parquet = output_data + "artists.parquet"
    artists_table.write.parquet(artist_data_parquet, mode="overwrite")

def process_log_data(spark, input_data, input_data2, output_data):
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    df.dropDuplicates()
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    df.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql(""" SELECT DISTINCT(userId), firstName, lastName, gender, level FROM log_data""")
    
    # write users table to parquet files
    user_data_parquet = output_data + "users.parquet"
    users_table.write.parquet(user_data_parquet, mode="overwrite")
    
    #create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    funcWeekDay =  udf(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%w'))
    df = df.withColumn('shortdate', col('datetime').substr(1,10)).withColumn('weekday', funcWeekDay(col('shortdate'))).drop('shortdate')
    df.createOrReplaceTempView("time_data")
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT datetime as start_time, EXTRACT(hour FROM datetime) AS hour,
                                                             EXTRACT(day FROM datetime) AS day,
                                                             EXTRACT(week FROM datetime) AS week,
                                                             EXTRACT(month FROM datetime) AS month,
                                                             EXTRACT(year FROM datetime) AS year,
                                                             weekday
                                                             FROM time_data""")
    
    # write time table to parquet files partitioned by year and month
    time_data_parquet = output_data + "time.parquet"
    time_table.write.partitionBy("year","month").parquet(time_data_parquet, mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data2)
    song_df.dropDuplicates()
    song_df.createOrReplaceTempView("song_data_log")
    
    # extract columns from joined song and log datasets to create songplays table 
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    df.createOrReplaceTempView("song_play_data")
    
    songplays_table = spark.sql("""SELECT songplay_id,datetime as starttime, userId, level, song_id, artist_id, sessionId, location,   userAgent, EXTRACT(month FROM datetime) AS month,EXTRACT(year FROM datetime) AS year
                                          FROM song_play_data t1
                                          JOIN song_data_log t2
                                          ON t1.artist = t2.artist_name AND
                                             t1.song = t2.title""")
    
    # write songplays table to parquet files partitioned by year and month
    songplay_data_parquet = output_data + "songplays.parquet"
    songplays_table.write.partitionBy("year","month").parquet(songplay_data_parquet, mode="overwrite")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    output_data = "s3a://dend-siva-datalake/"
    process_song_data(spark, input_data, output_data)  
    
    input_data = "s3a://udacity-dend/log_data/*/*/*.json"
    input_data2 = "s3a://udacity-dend/song_data/*/*/*/*.json"
    process_log_data(spark, input_data, input_data2, output_data)


if __name__ == "__main__":
    main()
