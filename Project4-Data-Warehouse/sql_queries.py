import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Redshift data types identified using AWS documentation
    # https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist varchar
        , auth varchar
        , firstName varchar
        , gender varchar
        , iteminSession smallint
        , lastName varchar
        , length double precision
        , level varchar
        , location varchar
        , method varchar
        , page varchar
        , registration double precision
        , sessionId integer
        , song varchar sortkey
        , status integer
        , ts double precision
        , userAgent varchar
        , userId integer

    )
    """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs smallint
        , artist_id varchar
        , artist_latitude double precision
        , artist_longitude double precision
        , artist_location varchar
        , artist_name varchar
        , song_id varchar
        , title varchar sortkey
        , duration double precision
        , year integer
    )
    """)

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id integer identity(0,1) PRIMARY KEY
        , start_time timestamp NOT NULL sortkey
        , user_id integer NOT NULL distkey
        , level varchar
        , song_id varchar
        , artist_id varchar
        , session_id integer
        , location varchar
        , user_agent varchar
    )
    """

user_table_create = """
    CREATE TABLE IF NOT EXISTS users
    (
        user_id integer PRIMARY KEY distkey
        , first_name varchar NOT NULL
        , last_name varchar NOT NULL
        , gender varchar
        , level varchar NOT NULL
    )
    """

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id varchar PRIMARY KEY 
        , title varchar NOT NULL
        , artist_id varchar NOT NULL sortkey
        , year integer
        , duration double precision NOT NULL
    )
    """

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id varchar PRIMARY KEY sortkey
        , name varchar NOT NULL
        , location varchar
        , latitude double precision
        , longitude double precision
    )
    """

time_table_create = """
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp PRIMARY KEY sortkey
        , hour integer
        , day integer
        , week integer
        , month integer
        , year integer
        , weekday integer
    )
    """

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
format as json {} region 'us-west-2'
""").format(config['S3']['log_data'],config['IAM_ROLE']['ARN'],config['S3']['log_jsonpath'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
json region 'us-west-2'
""").format(config['S3']['song_data'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

# Note:  TIMESTAMP function taken from stackoverflow, https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + stagingevents.ts/1000 * INTERVAL '1 second' AS start_time
        , se.userId as user_id
        , se.level
        , ss.song_id
        , ss.artist_id
        , se.sessionId as session_id
        , ss.artist_location as location
        , se.userAgent as user_agent
    FROM staging_events as se
    JOIN staging_songs as ss
    ON se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
    """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userId as user_id
        , firstName as first_name
        , lastName as last_name
        , gender
        , level
    FROM staging_events
    WHERE page = "NextSong"
    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM staging_songs
    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id
        , artist_name as name
        , artist_location as location
        , artist_latitude as latitude
        , artist_longitude as longitude
    FROM staging_songs
    """)

# Note:  EXTRACT functions syntax taken from AWS documentation, https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
# Note:  TIMESTAMP function taken from stackoverflow, https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + stagingevents.ts/1000 * INTERVAL '1 second' AS start_time
        , extract(hour from start_time) as hour
        , extract(day from start_time) as day
        , extract(week from start_time) as week
        , extract(month from start_time) as month
        , extract(year from start_time) as year
        , extract(weekday from start_time) as weekday
    FROM staging_events
    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
