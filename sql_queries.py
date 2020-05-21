import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLE

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS dimUser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
  artist text,
  auth text, 
  firstName text,
  gender varchar(1),
  itemInSession smallint,
  lastName text,
  length float,
  level text,
  location text,
  method text,
  page text,
  registration float,
  sessionId text,
  song text,
  status smallint,
  ts bigint,
  userAgent text,
  userId bigint
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
  num_songs bigint, 
  artist_id text,
  artist_latitude float, 
  artist_longitude float, 
  artist_location text,
  artist_name text,
  song_id text, title text,
  duration float, 
  year smallint
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongplay
(
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp DISTKEY,
    user_id bigint,
    level text,
    song_id text,
    artist_id text,
    session_id text,
    location text,
    user_agent text
    )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser
(
    user_id bigint SORTKEY,
    first_name text,
    last_name text,
    gender text,
    level text
    )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong
(
    song_id text,
    title text,
    artist_id text,
    year smallint,
    duration float
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist
(
    artist_id text,
    name text,
    location text,
    latitude float,
    longitude float
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime
(
    start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
    )
""")

# STAGING tABLES

staging_events_copy = f"""COPY staging_event FROM {config.get('S3', 'LOG_DATA')}
credentials 'aws_iam_role={config.get('IAM_ROLE', "ARN")}'
JSON 'auto'
region 'us-west-2' TRUNCATECOLUMNS;
"""

staging_songs_copy = f"""COPY staging_songs FROM {config.get('S3', 'SONG_DATA')}
credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}' fORMAT AS JSON 'auto'
region 'us-west-2' TRUNCATECOLUMNS;
"""

# fINAL tABLES

songplay_table_insert = ("""
INSERT INTO factSongplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT tIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent 
    FROM staging_events se
    INNER JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong' AND start_time IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO dimUser(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id, 
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level 
                    FROM staging_events se;
""")

song_table_insert = ("""INSERT INTO dimSong(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, 
                    artist_id,
                    year,
                    duration FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO dimArtist(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name AS name, 
                    artist_location AS location,
                    artist_latitude AS latitude, 
                    artist_longitude AS longitude FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO dimTime(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT tIMESTAMP 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' AS start_time,
        EXTRACT (hour FROM start_time),
        EXTRACT (day FROM start_time),
        EXTRACT (week FROM start_time),
        EXTRACT (month FROM start_time),
        EXTRACT (year FROM start_time),
        EXTRACT (weekday FROM start_time) 
        FROM staging_events AS se
        WHERE ts is not NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
