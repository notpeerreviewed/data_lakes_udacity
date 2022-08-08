import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Purpose: 
        Reads song and artist data files
        Inserts song and artist data into respective parquet files
        
    Parameters:
        - spark: spark session object
        - input_data: path to data files (Udacity S3 bucket)
        - output_data: path to output files (my S3 bucket, or local storage for testing)
        
    Returns:
        NIL
    """

    # get filepath to song data file
    # used abbreviated file set for testing
    # song_data = os.path.join(input_data, 'song_data', 'A', 'A', '*')
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('full_data')

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_latitude as latitude,
            artist_longitude as longitude,
            artist_location as location,
            artist_name as name 
        FROM full_data
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):

    """
    Purpose: 
        Reads log data files
        Generates user, time, and songplays tables and writes them to respective parquet files
        
    Parameters:
        - spark: spark session object
        - input_data: path to data files (Udacity S3 bucket)
        - output_data: path to output files (my S3 bucket, or local storage for testing)
        
    Returns:
        NIL
    """


    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*', '*')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    df.createOrReplaceTempView('full_log_data')

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT
            userId,
            firstName,
            lastName,
            gender,
            level
        FROM full_log_data
    """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('start_time', 'datetime')
    time_table = time_table.withColumn('hour', F.hour('start_time'))
    time_table = time_table.withColumn('day', F.dayofmonth('start_time'))
    time_table = time_table.withColumn('week', F.weekofyear('start_time'))
    time_table = time_table.withColumn('month', F.month('start_time'))
    time_table = time_table.withColumn('year', F.year('start_time'))
    time_table = time_table.withColumn('weekday', F.dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    # used abbreviated file set for testing
    # song_data = os.path.join(input_data, 'song_data', 'A', 'A', '*')
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))
    
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                   .parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-jeffoutput/"

    # used local drive for testing
    # output_data = "./spark-warehouse"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
