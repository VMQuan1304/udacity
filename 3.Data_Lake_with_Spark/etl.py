import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This func is use to create a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This func is use to process data from song data. 
    Write data into songs_table and artists_table
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('song')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_latitude', 'latitude') \
                    .withColumnRenamed('artist_longitude', 'longitude') \
                    .dropDuplicates()
    artists_table.createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This func is use to process data from log data. 
    Write data into users_table, time_table and songplays_table
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    temp_df = df.filter(df.page == 'NextSong') \
        .select('ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    users_table.createOrReplaceTempView('users')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    temp_df = temp_df.withColumn('datetime', get_timestamp(temp_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    temp_df = temp_df.withColumn('datetime', get_datetime(temp_df.ts))
    
    # extract columns to create time table
    time_table = temp_df.select('datetime') \
                .withColumn('start_time', temp_df.datetime) \
                .withColumn('hour', hour('datetime')) \
                .withColumn('day', dayofmonth('datetime')) \
                .withColumn('week', weekofyear('datetime')) \
                .withColumn('month', month('datetime')) \
                .withColumn('year', year('datetime')) \
                .withColumn('weekday', dayofweek('datetime')) \
                .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    
    temp_df = temp_df.alias('log_df')
    song_df = song_df.alias('song_df')
    joined_df = temp_df.join(song_df, col('log_df.artist') == col('song_df.artist_name'), 'inner')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select(
    col('log_df.datetime').alias('start_time'),
    col('log_df.userId').alias('userId'),
    col('song_df.song_id').alias('song_id'),
    col('song_df.artist_id').alias('artist_id'),
    col('log_df.sessionId').alias('sessionId'),
    col('log_df.location').alias('location'),
    col('log_df.userAgent').alias('userAgent'),
    year('log_df.datetime').alias('year'),
    month('log_df.datetime').alias('month')) \
    .withColumn('songplay_id', monotonically_increasing_id())

    songplays_table.createOrReplaceTempView('songplays')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    """
    Provide input and output data on S3
    Excute func process_song_data and process_log_data
    """
    spark = create_spark_session()
    input_data = "s3://quanvm4/"
    output_data = "s3://quanvm4/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
