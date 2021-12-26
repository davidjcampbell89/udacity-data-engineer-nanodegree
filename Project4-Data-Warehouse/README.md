## Project: Data Warehouse

#### Contents
This readme file outlines the background to the Project and details to allow a user to run the ETL process.  The contents include:
1. Background & Task
2. Star Schema
3. Input data files
4. Scripts: Details
5. Scripts: Steps to run ETL process

#### 1. Background & Task
A music streaming startup company called Sparkify has expanded their user base and song database and now want to move processes and data on to the cloud.  The data currently resides in S3 and they have a directory of JSON logs of user activity and a directory with JSON metadata on the songs in their app.

The task is for the data engineer to build an ETL pipeline to extract data from S3, stage it in Redshift, transform it into fact and dimensional tables in order to allow the analytics team to continue doing insights analysis on user behaviour.

#### 2. Star Schema

The database is created using a star schema with songplays as the fact table and users, songs, artists and time as the dimension tables. 

- *songplays* table contains the records of songs played by users and includes the following fields
    - **songplay_id(PK)**
    - start_time (sortkey)
    - user_id (distkey)
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent
    
- *users* table contains details of the users of the Sparkify platform
    - **user_id (PK) (distkey)**
    - first_name
    - last_name
    - gender
    - level

- *songs* table contains details of songs that exist on the Sparkify platform
    - **song_id (PK) (sortkey)**
    - title
    - artist_id
    - year
    - duration

- *artists* table contains details of artists that exist on the Sparkify platform
    - **artist_id (PK) (sortkey)**
    - name
    - location
    - latitude
    - longitude

- *time* table contains broken down timestamp fields related to the songplays table
    - **start_time (PK) (sortkey)**
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

There is no common primary key between the song files and the log files and therefore the song name, artist name and song duration are used to join these tables where required within the ETL.

#### 4. Scripts: Details

This repository contains the following scripts,
- create_tables.py - This script establishes a connection to the Redshift cluster that has been created to house the sparkify data warehouse and this script creates and drops the tables within Redshift.  This script references the sql_queries.py script for CREATE TABLE and DROP TABLE statements
- etl.py - This script performs the ETL, with data from S3 copied across to a staging area in Redshift and then inserted into the tables in the Redshift data warehouse as created by the create_tables.py script.  These tables can then be access by the analytics team to generate insights analysis
- sql_queries.py - This script creates string objects in the form of sql codes (DROP, CREATE, COPY, INSERT and SELECT statements) which are then used within etl.py and create_tables.py.  The CREATE statements ensure that the tables are structured as per schema requirements, with the INSERT statements then populating these tables.  The COPY statements are used for moving data from the S3 buckets to the Redshift staging area.  The staging tables created are `staging_events` and `staging_songs`
- README.md - This file is a readme file with information on the background to the project and information on how to run the ETL process
- run_scripts.ipynb - This notebook has been used to run the create_tables.py and etl.py scripts to support development work

#### 5. Scripts: Steps to run ETL process
To run the ETL process, the scripts should be run in the following order,
1. create_tables.py
2. etl.py