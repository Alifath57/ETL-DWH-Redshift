import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES IF EXIST

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
#Creat tables if not exist (All columns from the events data)
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR, \
                                                                auth VARCHAR, \
                                                                first_name VARCHAR, \
                                                                gender VARCHAR, \
                                                                item_in_session INTEGER, \
                                                                last_name VARCHAR, \
                                                                duration REAL, \
                                                                level VARCHAR, \
                                                                location VARCHAR, \
                                                                method VARCHAR, \
                                                                page VARCHAR, \
                                                                registration REAL, \
                                                                session_id INTEGER, \
                                                                song VARCHAR, \
                                                                status INTEGER, \
                                                                start_time BIGINT, \
                                                                user_agent VARCHAR, \
                                                                user_id INTEGER)""")

# All columns from the song data
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs INTEGER, \
                                                               artist_id VARCHAR, \
                                                               artist_latitude REAL, \
                                                               artist_longitude REAL, \
                                                               artist_location VARCHAR, \
                                                               artist_name VARCHAR, \
                                                               song_id VARCHAR, \
                                                               title VARCHAR, \
                                                               duration REAL, \
                                                               year INTEGER)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INTEGER NOT NULL PRIMARY KEY, \
                                                                start_time TIMESTAMP NOT NULL sortkey, \
                                                                user_id INTEGER NOT NULL, \
                                                                level VARCHAR, \
                                                                song_id VARCHAR, \
                                                                artist_id VARCHAR, \
                                                                session_id INTEGER NOT NULL, \
                                                                location VARCHAR, \
                                                                user_agent VARCHAR)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER NOT NULL PRIMARY KEY sortkey, \
                                                        first_name VARCHAR, \
                                                        last_name VARCHAR, \
                                                        gender VARCHAR, \
                                                        level VARCHAR)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR NOT NULL PRIMARY KEY, \
                                                        title VARCHAR sortkey, \
                                                        artist_id VARCHAR, \
                                                        year INTEGER, \
                                                        duration REAL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR NOT NULL PRIMARY KEY, \
                                                            name VARCHAR sortkey, \
                                                            location VARCHAR, \
                                                            latitude REAL, \
                                                            longitude REAL)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP NOT NULL PRIMARY KEY sortkey, \
                                                          hour INTEGER, \
                                                          day INTEGER, \
                                                          week INTEGER, \
                                                          month INTEGER, \
                                                          year INTEGER, \
                                                          weekday INTEGER)""")

# STAGING TABLES
# The JSON keys match the column names, so we can use the 'auto' format

staging_songs_copy = ("""copy staging_songs from {} 
                         iam_role '{}'
                         json 'auto';
                      """).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# For the staging_events table, we need to use a JSON paths filethe because the JSON key labels do not match the final column names.
staging_events_copy = ("""copy staging_events from {} 
                          iam_role '{}'
                          json {};
                       """).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

# FINAL TABLES

# Fact Table (songplays) 

songplay_table_insert = ("""
    INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
         SELECT
                 ROW_NUMBER() OVER (PARTITION BY 1),
                 TIMESTAMP 'epoch' + se.start_time::numeric/1000 *INTERVAL '1 second',
                 se.user_id,
                 se.level,
                 ss.song_id,
                 ss.artist_id,
                 se.session_id,
                 se.location,
                 se.user_agent
         FROM staging_events se
         LEFT JOIN staging_songs ss ON (se.artist = ss.artist_name) AND (se.song = ss.title)
         WHERE se.page = 'NextSong';
""")

# Dimensional tables (users, songs, artists, and time)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                      SELECT 
                       DISTINCT se.user_id,
                       se.first_name,
                       se.last_name,
                       se.gender,
                       se.level
                      FROM staging_events se
                      WHERE se.page= 'NextSong' AND se.user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                     SELECT DISTINCT 
                       ss.song_id,
                       ss.title,
                       ss.artist_id,
                       ss.year,
                       ss.duration
                     FROM staging_songs ss
                     WHERE ss.song_id IS NOT NULL
                     ORDER BY title
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                       SELECT 
                          DISTINCT ss.artist_id,
                          ss.artist_name,
                          ss.artist_location,
                          ss.artist_latitude,
                          ss.artist_longitude
                       FROM staging_songs ss
                       ORDER BY artist_name
""")



time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
           SELECT
                DISTINCT TIMESTAMP 'epoch' + start_time::numeric/1000 *INTERVAL '1 second' AS start_time_new,
                DATE_PART(hr, start_time_new),
                DATE_PART(d, start_time_new),
                DATE_PART(w, start_time_new),
                DATE_PART(mon, start_time_new),
                DATE_PART(yr, start_time_new),
                DATE_PART(dow, start_time_new)
           FROM staging_events
           WHERE page = 'NextSong'
           ORDER BY start_time
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]