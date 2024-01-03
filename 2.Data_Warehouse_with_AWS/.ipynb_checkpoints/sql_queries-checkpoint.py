import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_event(
    artist VARCHAR(255),
    auth VARCHAR(255),
    first_name VARCHAR(255),
    gender VARCHAR(1),
    item_in_session INT,
    last_name VARCHAR(255),
    length FLOAT,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration FLOAT,
    session_id INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    user_agent VARCHAR(255),
    user_id INT
    ) 
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_song(
    num_songs INT,
    artist_id VARCHAR(255),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(255),
    title VARCHAR(255),
    duration FLOAT,
    year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
    songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY, 
    user_id VARCHAR(255) NOT NULL DISTKEY, 
    level VARCHAR(255), 
    song_id VARCHAR(255), 
    artist_id VARCHAR(255), 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR
    ) diststyle key;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(255) PRIMARY KEY SORTKEY, 
    first_name VARCHAR(255), 
    last_name VARCHAR(255), 
    gender VARCHAR(255), 
    level VARCHAR(255)
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) NOT NULL PRIMARY KEY SORTKEY,
    title VARCHAR(255) NOT NULL, 
    artist_id VARCHAR(255), 
    year INT, 
    duration FLOAT NOT NULL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY, 
    name VARCHAR(255) NOT NULL, 
    location VARCHAR(255), 
    latitude FLOAT, 
    longitude FLOAT
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY, 
    hour INT, 
    day INT,
    week INT, 
    month INT, 
    year INT, 
    weekday INT
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stage_event from {bucket}
    iam_role '{iam_role}'
    region 'us-west-2'
    JSON {log_jsonpath}
""").format(bucket = LOG_DATA, iam_role = IAM_ROLE, log_jsonpath = LOG_JSONPATH)

staging_songs_copy = ("""
    copy stage_song from {bucket}
    iam_role '{iam_role}'
    region 'us-west-2'
    JSON 'auto'
""").format(bucket = SONG_DATA, iam_role = IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent
        )
    SELECT 
    TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id, 
    e.location, 
    e.user_agent
    FROM stage_event e
    LEFT JOIN stage_song s
    ON e.song = s.title AND
    e.artist = s.artist_name
    WHERE e.page = 'NextSong';
    
""")

user_table_insert = ("""
    INSERT INTO users(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
    )
    SELECT
    DISTINCT(user_id),
    first_name, 
    last_name, 
    gender, 
    level
    FROM stage_event
    WHERE user_id IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
    )
    SELECT
    DISTINCT(song_id), 
    title, 
    artist_id, 
    year, 
    duration
    FROM stage_song;
""")

artist_table_insert = ("""
    INSERT INTO artists(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
    )
    SELECT
    DISTINCT(artist_id),
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
    FROM stage_song;
""")

time_table_insert = ("""
    INSERT INTO time
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000)*INTERVAL '1 SECOND' AS ts FROM stage_event)
    SELECT 
    DISTINCT (ts),
    extract(hour from ts),
    extract(day from ts),
    extract(week from ts),
    extract(month from ts),
    extract(year from ts),
    extract(weekday from ts)
    FROM temp_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
