## Project: Data Lakes

#### Contents
This readme file outlines the background to the Project and details to allow a user to run the ETL process.  The contents include:
1. Background & Task
2. Star Schema
3. Input data files
4. Scripts: Details
5. Scripts: Steps to run ETL process

#### 1. Background & Task
A music streaming startup company called Sparkify has expanded their user base and song database and now want to move their data warehouse to a data lake.  The data currently resides in S3 and they have a directory of JSON logs of user activity and a directory with JSON metadata on the songs in their app.

The task is for the data engineer to build an ETL pipeline to extract data from S3, process using Spark and load back into S3 as a set of dimensional tables.  These tables are designed to allow the analytics team to continue doing insights analysis on user behaviour.

#### 2. Star Schema

The database is created using a star schema with songplays as the fact table and users, songs, artists and time as the dimension tables. 

- *songplays* table contains the records of songs played by users and includes the following fields
    - songplay_id
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent
    
- *users* table contains details of the users of the Sparkify platform
    - user_id
    - first_name
    - last_name
    - gender
    - level

- *songs* table contains details of songs that exist on the Sparkify platform
    - song_id
    - title
    - artist_id
    - year
    - duration

- *artists* table contains details of artists that exist on the Sparkify platform
    - artist_id
    - name
    - location
    - latitude
    - longitude

- *time* table contains broken down timestamp fields related to the songplays table
    - start_time
    - hour
    - day
    - week
    - month
    - year
    - weekday

#### 3. Input data files
The data exists in JSON files of songs and user listening logs and these are held in S3 buckets:
- Songs files `s3://udacity-dend/song_data` contain data about a song, including but not limited to song id, song title, artist id, artist name and song duration
- Log files `s3://udacity-dend/log_data` contain data about user acrtivity on the app, including but not limited to user id, user names, user location and then also song details including artist name, song name and song duration
- Data folder within this repository contains smaller datasets for song and log files.  These have been used to develop etl process before implementation on the S3 data.

There is no common primary key between the song files and the log files and therefore the song name, artist name and song duration are used to join these tables where required within the ETL.

#### 4. Scripts: Details

This repository contains the following scripts,
- etl.py - This script performs the ETL, with data from S3 process using Spark and load back into S3 as a set of dimensional tables in parquet files in a data lake.  These tables can then be accessed by the analytics team to generate insights analysis
- README.md - This file is a readme file with information on the background to the project and information on how to run the ETL process
- dev.ipynb - This notebook has been used to develop the code underpinning the process and this code has then been implemented in the etl.py file
- dl.cfg - This cfg file is populated by the user with the access key and secret access key required to connect to AWS.  User keys have been removed from this repository

#### 5. Scripts: Steps to run ETL process
To run the ETL process, the following steps should be followed,
1. dl.cfg - populate with user AWS credentials
2. etl.py - run etl script