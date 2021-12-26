# import packages
import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date
from pyspark.sql.types import IntegerType, StringType, TimestampType, DateType

# configure session
config = configparser.ConfigParser()
config.read('dl.cfg')

# configure AWS connection
os.environ['AWS_ACCESS_KEY_ID']=['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=['AWS']['AWS_SECRET_ACCESS_KEY']

# configure session
def create_spark_session():
    """
    Configure and build SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    - Read data from song data files
    - Select columns required for songs and artists tables
    - Write songs and artists tables to parquet files
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df_song = spark.read.json(song_data)
    
    # create view of song_data dataframe from which we will extract the required columns
    df_song.createOrReplaceTempView("df_song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM df_song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "song_table.parquet")
    
    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT
        artist_id
        , artist_name as name
        , artist_location as location
        , artist_latitude as latitude
        , artist_longitude as longitude
    FROM df_song_data
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artist_table.parquet")

def process_log_data(spark, input_data, output_data):
    """
    - Read data from log data files
    - Select columns required for songs and artists tables
    - Write songs and artists tables to parquet files
    """
    # get filepath to log data file
    log_data = "log_data/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log["page"] == "NextSong")

    # Create view of df_log from which to access the columns
    df_log.createOrReplaceTempView("df_log_data")

    # extract columns for users table
    users_table = spark.sql("""
    SELECT DISTINCT 
        userId as user_id
        , firstName as first_name
        , lastName as last_name
        , gender
        , level
    FROM df_log_data
    """)
    
    #user_id is a string but could be an integer, update the datatype to integer
    users_table = users_table.withColumn("user_id", users_table["user_id"].cast(IntegerType()))

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    df_timestamp = df_log.withColumn("timestamp", (df_log["ts"]/1000).cast(dataType=TimestampType()))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    df_datetime = df_timestamp.withColumn("datetime", to_date(df_timestamp["timestamp"]))
    
    # Create view of df_datetime from which to access the columns
    df_datetime.createOrReplaceTempView("df_datetime_data")
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT DISTINCT
        timestamp AS start_time
        , extract(hour from timestamp) as hour
        , extract(day from timestamp) as day
        , extract(week from timestamp) as week
        , extract(month from timestamp) as month
        , extract(year from timestamp) as year
        , extract(dayofweek from timestamp) as weekday
    FROM df_datetime_data
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "time_table.parquet")

    # get filepath to song data file
    song_data = "data/song-data.zip"
    
    # read in song data to use for songplays table
    df_song = spark.read.json(song_data)

    # create view of song_data dataframe from which we will extract the required columns
    df_song.createOrReplaceTempView("df_song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT 
        d.timestamp AS start_time
        , d.userId as user_id
        , d.level
        , s.song_id
        , s.artist_id
        , d.sessionId as session_id
        , s.artist_location as location
        , d.userAgent as user_agent
    FROM df_datetime_data as d
    JOIN df_song_data as s
    ON d.song = s.title
    AND d.artist = s.artist_name
    AND d.length = s.duration
    """)

    #user_id is a string but could be an integer, update the datatype to integer
    songplays_table = songplays_table.withColumn("user_id", songplays_table["user_id"].cast(IntegerType()))

    # write songplays table to parquet files (partitioned by artist_id)
    songplays_table.write.mode("overwrite").partitionBy("artist_id").parquet(output_data + "songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dc-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
