import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function is responsible to create or gate an SparkSession and use the aws credentials
    to connect to the data source s3 to consume and load data.
    
    Arguments:
        None
        
    Return:
        spark: SparkSession object
    """
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function is responsible to process song data through reading data from s3 bucket, then
    processing it with apache spark dataframe to create song and artist table. Finally write it down as parquet file into S3
    Arguments:
        - spark: SparkSession Object to process the data
        - input_data: String with the s3 URL to get the data
        - output_data: String with the 3 URL to write the data
    Return:
        None
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','artist_id','title','year','duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id') \
                .mode('overwrite') \
                .parquet(os.path.join(output_data, "songs/songs.parquet"))

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name', 'artist_location','artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, "artists/artists.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    Description: This function is responsible to process log data through reading data from s3 bucket, then
    processing it with apache spark dataframe to create songplay, time, users, and artist table. Finally write it down as parquet file into S3
    Arguments:
        - spark: SparkSession Object to process the data
        - input_data: String with the s3 URL to get the data
        - output_data: String with the 3 URL to write the data
    Return:
        None
    """
    # get filepath to log data file
    log_data =  os.path.join("s3a://udacity-dend/", "log-data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, "users/users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df['ts']))
    
    # extract columns to create time table
    time_table = df.withColumn('hour',       hour(df['start_time'])) \
        .withColumn('day',        dayofmonth(df['start_time'])) \
        .withColumn('week',       weekofyear(df['start_time'])) \
        .withColumn('month',      month(df['start_time'])) \
        .withColumn('year',       year(df['start_time'])) \
        .withColumn('weekday',    dayofweek(df['start_time'])) \
        .select('start_time', 'hour', 'day','week', 'month', 'year', 'weekday') \
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite').parquet(os.path.join(output_data, "time/time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/A/A/A/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer')\
        .select(
            monotonically_increasing_id().alias('songplay_id'),
            df.ts,
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('start_time').alias('year'),
            month('start_time').alias('month')
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite').parquet(os.path.join(output_data, "songplays/songplays.parquet"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakedendp/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
