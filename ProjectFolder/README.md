## Data Lake Project

### Description

- A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
  In this project we will build an ETL pipeline that extracts the data from S3 process the data into analytics tables and save that back to an S3 bucket.

### Deployment

- dl.cfg is a configuration file. Create your own with your AWS user credentials in the root folder containing:

  [AWS] \
  AWS_ACCESS_KEY_ID = YOUR_AWS_ACCESS_KEY \
  AWS_SECRET_ACCESS_KEY = YOUR_AWS_SECRET_KEY

- **Local Deployment:**

  - In terminal at root folder location run $ python etl.py

- **Deployment via EMR:**

  - Create an EMR cluster either via console or terminal commands.
    <p align="left" ></p><img src="AWS_EMR_Create_Cluster_Quick_Options.png" width="200" height="200"></p>
    - **NOTE:** If creating cluster with CLI make sure --release-label emr-5.20.0
    - If using one > than emr-5.20.0 I ran into errors.
  - Make sure you copy the .pem file, dl.cfg and the etl.py file into the EMR.
    - $ scp -i < .pem-file > < Local-File-Path > hadoop@< Master public DNS >:/home/hadoop/
  - Log into the EMR cluster via ssh
    - $ ssh -i spark-cluster-emr.pem hadoop@< Master public DNS >
  - Install package configparser if not in the $ pip list
    - $ sudo pip install configparser
  - Run the spark job
    - $ spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2 --conf fs.s3a.multipart.size=104857600

### Datasets

1. **Song data:**

   - Location: s3://udacity-dend/song_data
   - Content: Data is broken up in folders and saved as JSON files containing the metadata about a song and artist of that song.
   - File path example: song_data/A/B/C/TRABCEI128F424C983.json

1. **Log data:**
   - Location: s3://udacity-dend/log_data
   - Content: Data consists of log files in JSON format. They simulate app activity logs from an imaginary music streaming app.
   - File path example: log_data/2018/11/2018-11-12-events.json

### ETL Pipeling

1. Read the data from S3
2. Process the data using spark
3. Save the processed data as parquet file tables back to S3

### Schema and Tables

- **Fact Table**
  1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
     - _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_
- **Dimension Tables**
  1. **users** - users in the app
     - _user_id, first_name, last_name, gender, level_
  2. **songs** - songs in music database
     - _song_id, title, artist_id, year, duration_
  3. **artists** - artists in music database
     - _artist_id, name, location, lattitude, longitude_
  4. **time** - timestamps of records in **songplays** broken down into specific units
     - _start_time, hour, day, week, month, year, weekday_
