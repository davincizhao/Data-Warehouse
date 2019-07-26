import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP table  IF EXISTS staging_songs_table"
songplay_table_drop = "DROP table IF EXISTS songplay_table"
user_table_drop = "DROP table IF EXISTS user_table"
song_table_drop = "DROP table IF EXISTS song_table"
artist_table_drop = "DROP table IF EXISTS artist_table"
time_table_drop = "DROP table IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table \
                                (artist varchar(255) encode text255,
                                auth varchar(255) encode text255,
                                firstName varchar(100),
                                gender varchar(1),
                                itemInSession integer,
                                lastName varchar(100),
                                length DOUBLE PRECISION,
                                level varchar(20),
                                location varchar(255) encode text255,
                                method varchar(10),
                                page varchar(50),
                                registration varchar(100),
                                sessionId integer,
                                song varchar(200),
                                status integer,
                                ts bigint,
                                userAgent varchar(255) encode text255,
                                userId integer)
                                diststyle even;
                                """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table \
                                (num_songs int , 
                                artist_id varchar , 
                                latitude float, 
                                longitude float,
                                location varchar(255) encode text255,
                                artist_name varchar,
                                song_id varchar PRIMARY KEY ,
                                title varchar ,
                                duration float,
                                year int);
""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay_table \
                        ( songplay_id bigint identity(0, 1),
                        start_time bigint NOT NULL, 
                        user_id int NOT NULL, 
                        level varchar , 
                        song_id varchar , 
                        artist_id varchar , 
                        session_id int NOT NULL, 
                        location varchar, 
                        user_agent varchar,
                        primary key(songplay_id))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user_table \
                        (user_id int PRIMARY KEY NOT NULL sortkey, 
                        first_name varchar, 
                        last_name varchar, 
                        gender varchar, 
                        level varchar)
                        diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song_table \
                        (song_id varchar PRIMARY KEY sortkey, 
                        title varchar NOT NULL, 
                        artist_id varchar, 
                        year int, 
                        duration float)
                        diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist_table \
                        (artist_id varchar PRIMARY KEY sortkey, 
                        artist_name varchar, 
                        location varchar, 
                        latitude float, 
                        longitude float)
                        diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table \
                        (t_ts bigint ,
                        t_start_time timestamp PRIMARY KEY sortkey ,
                        t_hour int, 
                        t_day int, 
                        t_week int, 
                        t_month int, 
                        t_year int, 
                        t_weekday int)
                        diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""COPY {} FROM {}
                          credentials 'aws_iam_role={}'
                          FORMAT AS  json  {}
                          region 'us-west-2'                    
""").format("staging_events_table", config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY {} FROM {}
                         credentials 'aws_iam_role={}'
                         region 'us-west-2'
                         FORMAT as JSON 'auto'
                         
""").format("staging_songs_table",config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay_table ( start_time , user_id , level, 
                        song_id , artist_id , session_id , location , user_agent) 
                        SELECT DISTINCT staging_events_table.ts, 
                        staging_events_table.userId, 
                        staging_events_table.level, 
                        staging_songs_table.song_id, 
                        staging_songs_table.artist_id, 
                        staging_events_table.sessionId, 
                        staging_events_table.location, 
                        staging_events_table.userAgent 
                        FROM staging_events_table JOIN staging_songs_table ON 
                        (staging_songs_table.title = staging_events_table.song 
                        AND staging_songs_table.artist_name = staging_events_table.artist 
                        AND staging_songs_table.duration = staging_events_table.length)
                        WHERE staging_events_table.page = 'NextSong'
                         
                        
                        """)

user_table_insert = (""" INSERT INTO user_table (user_id , first_name , last_name , gender , level ) \
                    SELECT DISTINCT
                    userId , 
                    firstName , 
                    lastName , 
                    gender , 
                    level
                    FROM staging_events_table
                    WHERE staging_events_table.page = 'NextSong'
                    """)

song_table_insert = (""" INSERT INTO song_table (song_id , title , artist_id , year , duration ) \
                    SELECT DISTINCT
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    FROM staging_songs_table
                    """)

artist_table_insert = (""" INSERT INTO artist_table (artist_id , artist_name , location , latitude , longitude )\
                    SELECT DISTINCT
                    artist_id , 
                    artist_name , 
                    location , 
                    latitude , 
                    longitude
                    FROM staging_songs_table
                    """)


time_table_insert = (""" INSERT INTO time_table (t_ts,t_start_time,t_hour,t_day,t_week,t_month,t_year,t_weekday)
                    Select DISTINCT start_time
                    ,t_start_time
                    ,EXTRACT(HOUR FROM t_start_time) As t_hour
                    ,EXTRACT(DAY FROM t_start_time) As t_day
                    ,EXTRACT(WEEK FROM t_start_time) As t_week
                    ,EXTRACT(MONTH FROM t_start_time) As t_month
                    ,EXTRACT(YEAR FROM t_start_time) As t_year
                    ,EXTRACT(DOW FROM t_start_time) As t_weekday
                    FROM (
                    SELECT distinct start_time,'1970-01-01'::date + start_time/1000 * interval '1 second' as t_start_time
                    FROM songplay_table)
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries =[songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
