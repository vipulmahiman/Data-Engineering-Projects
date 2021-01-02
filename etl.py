import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2")\
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process the songs data files and create songs and artist dimension tables.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    """
    
    # read songs data file in staging table
    df_staging_songs = spark.read.json(input_data)
    df_staging_songs = df_staging_songs.withColumnRenamed("year","song_year")
    print('Staging_Songs Count --> ', df_staging_songs.count())
    df_staging_songs.show(5)
    
    df_staging_songs.createOrReplaceTempView("staging_songs")
    songs_table = spark.sql("""select
        distinct song_id,
        title,
        artist_id,
        song_year,
        duration
    from
    staging_songs""")

    songs_table.show()

     # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("song_year","artist_id")\
    .parquet(output_data + "songs/") 
    
    print('Songs_table Count --> ', songs_table.count())

    # extract columns to create artists table
    artist_table = spark.sql("""select
        distinct artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from
    staging_songs""")
      
    artist_table.show()
    # write artists table to parquet files
    artist_table.write.mode('overwrite').parquet(output_data + "artists/") 

    print('artist_table Count --> ', artist_table.count())
    

def process_log_data(spark, input_data, output_data):
    """Process the events data files and create users, times dimension and songplay
    fact table.
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    """
    
    # read log data file
   
    df_staging_events = spark.read.json(input_data)
    print('Before NextSong Filter --> ',df_staging_events.count())

    df_staging_events = df_staging_events.filter("page == 'NextSong'")
    print('After NextSong Filter --> ',df_staging_events.count())

    df_staging_events.printSchema()
    df_staging_events.show(5)
    

    # create timestamp column from original timestamp column
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts/1000)
    
    df_staging_events = df_staging_events.withColumn("timestamp",get_timestamp(df_staging_events.ts))
    df_staging_events.printSchema()
    df_staging_events.show(5)
    
    # create datetime column from original timestamp column
    @udf(t.StringType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')

    df_staging_events = df_staging_events.withColumn("datetime",get_timestamp(df_staging_events.ts))
    df_staging_events = df_staging_events.withColumn("year",year(df_staging_events.datetime))\
    .withColumn("month",month(df_staging_events.datetime))
    df_staging_events.printSchema()
    df_staging_events.show(5)
    
    df_staging_events.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql("""select 
        distinct userid,
        firstname,
        lastname,
        gender,
        level
    from
    staging_events
    """)
    users_table.show()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users/") 
    
    print('users_table Count --> ', users_table.count())
    
    # read Songs Parquet data
    df_songs = spark.read.parquet(output_data + "songs/")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    df_songplay = df_staging_events.join\
    (df_songs, df_staging_events.song == df_songs.title, "left")
    #df_songplay.printSchema()
    df_songplay = df_songplay.select("datetime","year","month", "userId",\
                                     "level","song_id","artist_id","sessionId","location","useragent")
    df_songplay = df_songplay.withColumn("songplay_id", F.monotonically_increasing_id())
    #df_songplay.show()

    # write songplays table to parquet files partitioned by year and month
    df_songplay.write.mode('overwrite').partitionBy("year","month").\
    parquet(output_data + "songplay/") 

    #print('df_songplay Count --> ', df_songplay.count())
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT 
        distinct datetime as start_time,
        hour(datetime) as hour,
        dayofyear(datetime) as day,
        weekofyear(datetime) as week,
        month(datetime) as month,
        year(datetime) as year,
        dayofweek(datetime) as weekday
    from
    staging_events
    where page='NextSong'
    """)

    #time_table.show()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").\
    parquet(output_data + "time/") 
    print('time_table Count --> ', time_table.count())


def main():
    spark = create_spark_session()
    
    output_path = config['FILES']['OUT_DATA_PATH']
   
    # get filepath to song data file
    song_data = config['FILES']['SONG_DATA_PATH']
    process_song_data(spark, song_data, output_path)    

    # get filepath to log data file
    log_data = config['FILES']['LOG_DATA_PATH']
    process_log_data(spark, log_data, output_path)


if __name__ == "__main__":
    main()
