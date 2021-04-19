# USED WITH SPARK CLUSTERS CREATED VIA THE CLI.
# STILL SOME ISSUES WHEN WRITE OR READ THE PARQUET FILES TO/FROM S3. ABLE TO SAVE THEM  LOCALLY

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as f

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "us-east-1.amazonaws.com")
    sc = spark.sparkContext
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()
    df.show(5)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    # songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])
    songs_table.write.partitionBy("year","artist_id").format('parquet').mode("overwrite").saveAsTable("songs_table")

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    artists_table.show(5)

    # write artists table to parquet files
    # artists_table.write.parquet(output_data + "artists/", mode="overwrite")
    artists_table.write.format('parquet').mode("overwrite").save("artists_table")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    dfLog = spark.read.json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    dfLog = dfLog.filter(dfLog.page == "NextSong")

    # extract columns for users table    
    users_table = dfLog.select("userId","firstName","lastName","gender","level").drop_duplicates()
    users_table.show(5)

    # write users table to parquet files
    # users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = f.udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    dfLog = dfLog.withColumn("start_time", get_timestamp("ts"))

    
    # extract columns to create time table
    time_table = dfLog.withColumn("hour",f.hour("start_time"))\
                .withColumn("day",f.dayofmonth("start_time"))\
                .withColumn("week",f.weekofyear("start_time"))\
                .withColumn("month",f.month("start_time"))\
                .withColumn("year",f.year("start_time"))\
                .withColumn("weekday",f.dayofweek("start_time"))\
                .select("start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()
                
    time_table.show(5)
    
    # write time table to parquet files partitioned by year and month
    # time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    # song_df = spark.read\
    #         .format("parquet")\
    #         .option("basePath", os.path.join(output_data, "songs/"))\
    #         .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    # songplays_table = dfLog.join(song_df, dfLog.song == song_df.title, how='inner')\
    #     .select(f.monotonically_increasing_id().alias("songplay_id"), f.col("start_time"), f.col("userId").alias("user_id"), \
    #         "level","song_id","artist_id", f.col("sessionId").alias("session_id"), "location", f.col("userAgent").alias("user_agent"))
    
    # write songplays table to parquet files partitioned by year and month
    # songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner", partitionBy=["year","month"])\
    #     .select("songplay_id", time_table.start_time.alias("start_time"), "user_id", "level", \
    #             "song_id", "artist_id", "session_id", "location", "user_agent", time_table.year, time_table.month)
    # songplays_table.show(5)

def main():

    spark = create_spark_session()
    input_data = "s3a://brad-data-01/demo-data/udacity-data/"
    # input_data = "s3a://udacity-dend/"
    output_data = "s3a://brad-data-01/demo-data/udacity-data/output_data/"
    # output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()