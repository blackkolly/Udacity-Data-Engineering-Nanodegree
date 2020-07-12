import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create  a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes the songs and artist tables
        and then load them back to S3
        
        Parameters:
            spark       = Spark Session
            input_data  = location of song_data files to be processed.
            output_data = path to location to store the output
                          (parquet files).
    """
    #filepath to the song data file
    
    song_data = input_data + 'song_data/*/*/*/*.json'
     # read song data file
    df = spark.read.json(song_data)
    
    # created song view to write SQL Queries
    df.createOrReplaceTempView("songs_table")
    
    # extract columns to create song
    songs_schema_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_schema_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    
    # extract columns to create artists table
    artists_schema_table = spark.sql(""" select distinct a.artist_id, a.artist_name, a.artist_location, a.artist_latitude, a.artist_longitude from songs_table a where a.artist_id IS NOT NULL """)
    
    # write artists table to parquet files
    artists_schema_table.write.mode('overwrite').parquet(output_data+'artists_schema_table/')




def process_log_data(spark, input_data, output_data):
    """
       Load JSON input data (log_data) from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
              spark     = Spark Session
            input_data  = location of song_data files to be processed.
            output_data = path to location to store the output
                          (parquet files).  
            
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    
    # read log data file

    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    
    log_df = log_df.filter(log_df.page == 'NextSong')
    # extract columns for users table 
    
    user_schema_table=spark.sql(""" select distinct l.userId as user_id, l.firstName as first_name, l.lastName as last_name, 
    l.gender as gender, l.level as level 
    from logs_table l WHERE l.userId IS NOT NULL """)
    
    # write users table to parquet files
    users_schema_table.write.mode('overwrite').parquet(output_data+'users_schema_table/')
    
     # extract columns to create time table
    
    get_datetime = udf(date_convert, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))

    time_table = df.select("start_time").dropDuplicates() \
        .withColumn("hour", hour(col("start_time")).withColumn("day", day(col("start_time")) \
        .withColumn("week", week(col("start_time")).withColumn("month", month(col("start_time")) \
        .withColumn("year", year(col("start_time")).withColumn("weekday", date_format(col("start_time"), 'E'))
                    
    # write time table to parquet files partitioned by year and month
                    
    songs_table.write.partitionBy("year", "month").parquet(output_data + 'time/')
                    
    # extract columns from joined song and log datasets to create songplays table
                    
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')

    artists_df = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))

    songplays = artists_logs.join(
        time_table,
        artists_logs.ts == time_table.start_time, 'left'
    ).drop(artists_logs.year)

    songplays_table = songplays.select(
        col('start_time').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'),
    ).repartition("year", "month")
                    
                    
    # write songplays table to parquet files partitioned by year and month

    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    """
        Main function
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)   
    print('song data processing complete: ' + str(datetime.now()))
    process_log_data(spark, input_data, output_data)
    print('ETL complete: ' + str(datetime.now()))


if __name__ == "__main__":
    main()
