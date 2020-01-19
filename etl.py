# to submit... https://knowledge.udacity.com/questions/46619

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('secret.cfg')
[KEY, SECRET] = config['AWS'].values()

os.environ['AWS_ACCESS_KEY_ID']=KEY #           config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=SECRET #    config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_all_data(spark, song_input_data, log_input_data, output_data):
    '''
    This function reads in song json data from the song_input_data location,
    transforms it into fact and dimension tables, and then saves it back to the 
    output_data location.
    
    INPUTS:
        spark (SparkSession): Spark session
        song_input_data (String): read location for song json data
        output_data (String): write location for song table data
    RETURNS:
        none
    '''
    print('##### PROCESSING SONG DATA #####')
    
    # read song data file
    df_song = spark.read.json(song_input_data) \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_name', 'name') \
        .dropDuplicates()
    df_song.printSchema()

    # extract columns to create songs table
    # song_id, title, artist_id, year, duration
    songs_table = df_song.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    print(f'writing {output_data}songs_table')
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(f'{output_data}songs_table')

    # extract columns to create artists table
    # artist_id, name, location, latitude, longitude
    artists_table = df_song.select('artist_id', 'name', 'location', 'latitude', 'longitude')
    
    # write artists table to parquet files
    print(f'writing {output_data}artists_table')
    artists_table.write.mode('overwrite').parquet(f'{output_data}artists_table')
    
    print('##### PROCESSING LOG DATA #####')

    # read log data file
    df_log = spark.read.json(log_input_data) \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent') \
        .withColumnRenamed('location', 'user_location')
    df_log.printSchema()
    
    # filter by actions for song plays
    df_log = df_log.where("page = 'NextSong'").dropDuplicates()

    # extract columns for users table
    # user_id, first_name, last_name, gender, level
    users_table = df_log.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    print(f'writing {output_data}users_table')
    users_table.write.mode('overwrite').parquet(f'{output_data}users_table')
    
    print('##### CREATING CUSTOM TIMESTAMP AND DATETIME COLUMNS #####')
    # create timestamp column from original timestamp column
    @udf(TimestampType())
    def get_timestamp(x):
        return datetime.fromtimestamp(x/1000.0)
    df_log = df_log.withColumn('ts_timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    df_log = df_log.withColumn('ts_datetime', col("ts_timestamp").cast(DateType()))
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_table = df_log.selectExpr("ts_timestamp as start_time")
    time_table = time_table.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', date_format('start_time', 'u'))
    
    # write time table to parquet files partitioned by year and month
    print(f'writing {output_data}time_table')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}time_table')
    
    # join datasets
    df_joined = df_log.join(df_song, (df_log.artist == df_song.name) & (df_log.song == df_song.title)) \
        .withColumn("songplay_id", monotonically_increasing_id())
    
    print('df_joined schema:')
    df_joined.printSchema()

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df_joined.select('songplay_id', 'ts_timestamp', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'user_location', 'user_agent') \
        .withColumnRenamed('ts_timestamp', 'start_time') \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time'))

    # write songplays table to parquet files partitioned by year and month
    print(f'writing {output_data}songplays_table')
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}songplays_table')
    

#def process_log_data(spark, song_input_data, log_input_data, output_data):



def main():
    
    # test mode
    # if running in test mode, make sure that the zip files in /data/ have been unzipped in place:
        # cd data
        # mkdir more
        # sudo apt-get install unzip
        # unzip song-data.zip -d more
        # unzip log-data.zip -d more/log_data
    test_mode = False
    
    if test_mode:
        print('##### RUNNING IN TEST MODE WITH LOCAL FILES #####')
        song_input_data = "data/more/song_data/*/*/*/*.json"
        log_input_data = "data/more/log_data/*.json"
        output_data = "data/more/"
    else:
        song_input_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
        log_input_data = "s3a://udacity-dend/log_data/*.json"
        output_data = "s3a://udacity-dend/"
        
    print('##### CREATING SPARK SESSION #####')
    spark = create_spark_session()
    
    process_all_data(spark, song_input_data, log_input_data, output_data)    
    #process_log_data(spark, song_input_data, log_input_data, output_data)


if __name__ == "__main__":
    main()
