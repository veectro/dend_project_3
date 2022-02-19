import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
REDSHIFT_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR, 
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR (1),
        itemInSession INT NOT NULL,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT NOT NULL,
        song VARCHAR,
        status INT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY , 
        start_time TIMESTAMP NOT NULL, 
        user_id INT NOT NULL, 
        level VARCHAR(5), 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT PRIMARY KEY NOT NULL, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR(1), 
        level VARCHAR(5)
    );
""")

song_table_create = ("""
   CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY NOT NULL, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year SMALLINT, 
        duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY, 
        name VARCHAR, 
        location VARCHAR, 
        latitude FLOAT, 
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY, 
        hour SMALLINT, 
        day SMALLINT, 
        week SMALLINT, 
        month SMALLINT, 
        year SMALLINT, 
        weekday SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events from {LOG_DATA}
    IAM_ROLE '{REDSHIFT_ARN}'
    FORMAT AS JSON {LOG_JSONPATH};
""")

staging_songs_copy = (f"""
    COPY staging_songs from {SONG_DATA}
    IAM_ROLE '{REDSHIFT_ARN}'
    FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id,
        session_id, location, user_agent
    )
    SELECT
        date_add('ms',e.ts,'1970-01-01') AS start_time,
        e.userId AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name, gender, level
    )
    SELECT distinct(e.userId), e.firstName, e.lastName, e.gender, e.level
        FROM staging_events e 
        WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, title, artist_id, year, duration
    )
    SELECT s.song_id, s.title, s.artist_id, s.year, s.duration
        FROM staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, name, location, latitude, longitude
    )
    SELECT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
        FROM staging_songs s;
""")

time_table_insert = ("""
    INSERT into time (
        start_time, hour, day, week, month, year, weekday
    )
    SELECT 
        date_add('ms',e.ts,'1970-01-01') AS start_time,
        EXTRACT(hour from e.start_time) AS hour,
        EXTRACT(day from e.start_time) AS day,
        EXTRACT(week from e.start_time) AS week,
        EXTRACT(month from e.start_time) AS month,
        EXTRACT(year from e.start_time) AS year,
        EXTRACT(dow from e.start_time) AS weekday    
    FROM staging_events e;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
