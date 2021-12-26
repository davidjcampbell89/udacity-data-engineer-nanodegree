## Project: Data Modelling with Postgres

#### Contents
This readme file outlines the background to the Project and details to allow a user to run the ETL process.  The contents include:
1. Background & Task
2. Star Schema
3. Input data files
4. Scripts: Details
5. Scripts: Steps to run ETL process


#### 1. Background & Task
A startup company called Sparkify has launched a new music streaming app.  They are interested in understanding the songs that users are currently listening to, however they do not currently have a database set up to allow for the analytics team to extract this information easily.

The task is for the data engineer to create a star schema database in Postgres with fact and dimension tables and build an ETL pipeline to transfer the data in from the JSON files holding the data.  The tables are to be designed to be optimised for the queries on song plays to support the analytics team in their analyses.

#### 2. Database Star Schema

The database is created using a star schema with songplays as the fact table and users, songs, artists and time as the dimension tables. 

- *songplays* table contains the records of songs played by users and includes the following fields
    - **songplay_id (PK)**
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent
    
- *users* table contains details of the users of the Sparkify platform
    - **user_id (PK)**
    - first_name
    - last_name
    - gender
    - level

- *songs* table contains details of songs that exist on the Sparkify platform
    - **song_id (PK)**
    - title
    - artist_id
    - year
    - duration

- *artists* table contains details of artists that exist on the Sparkify platform
    - **artist_id (PK)**
    - name
    - location
    - latitude
    - longitude

- *time* table contains broken down timestamp fields related to the songplays table
    - **start_time (PK)**
    - hour
    - day
    - week
    - month
    - year
    - weekday

#### 3. Input data files
The data exists in JSON files, with repositories of files for songs and for user listening logs:
- Songs files contain data about a song, including but not limited to song id, song title, artist id, artist name and song duration
- Log files contain data about user acrtivity on the app, including but not limited to user id, user names, user location and then also song details including artist name, song name and song duration

The data folder within this repository contains subfolders for song_data and log_data, holding the song and log data respectively.  There are further subfolders structures within each and these hold the JSON files with the data that is required for the ETL.

There is no common primary key between the song files and the log files and therefore the song name, artist name and song duration are used to join these tables where required within the ETL.

#### 4. Scripts: Details

This repository contains the following scripts,
- create_tables.py - This script creates the sparkify database and creates and drops the tables within the database.  This script references the sql_queries.py script for CREATE TABLE and DROP TABLE statements
- etl.ipynb - This notebook contains step by step workings of the ETL process and contains markup and comments to guide the reader through the code
- etl.py - This script performs the ETL, with data from the song and log JSON files loaded row by row into the tables in the sparkify database which was created by the create_tables.py script.  This script has been generated using the workings within the etl.ipynb notebook
- run_scripts.ipynb - This notebook has been used to run the create_tables.py and etl.py scripts to support development work
- sql_queries.py - This script creates string objects in the form of sql codes (DROP, CREATE, INSERT and SELECT statements) which are then used within etl.py and create_tables.py.  The CREATE statements ensure that the tables are structured as per schema requirements, with the INSERT statements then populating these tables.  The SELECT statement allows a JOIN to be performed between the songs and artists table such that relevant song metadata can be included alonside the user activity
- test.ipynb - This script allows the user to test that tables have been created as expected
- README.md - This file is a readme file with information on the background to the project and information on how to run the ETL process

#### 5. Scripts: Steps to run ETL process
To run the ETL process, the scripts should be run in the following order,
1. create_tables.py
2. etl.py

The 'run_scripts.ipynb' notebook within this repository can be used to run the ETL process.  The 'test.ipynb' notebook can then be used to test that the scripts have run correctly.


