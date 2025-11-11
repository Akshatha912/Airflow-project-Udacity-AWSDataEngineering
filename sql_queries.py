# plugins/helpers/sql_queries.py

class SqlQueries:
    """
    SQL helper class used by Airflow operators to load fact and dimension tables.
    """

    # ----------------------------------------------------------------------
    # Fact table: songplays
    # ----------------------------------------------------------------------
    songplay_table_insert = ("""
        INSERT INTO songplays (
            playid,
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
            md5(events.sessionid || events.start_time) AS playid,
            TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time,
            events.userid AS user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid AS session_id,
            events.location,
            events.useragent AS user_agent
        FROM staging_events AS events
        JOIN staging_songs AS songs
          ON events.song = songs.title
         AND events.artist = songs.artist_name
         AND events.length = songs.duration
        WHERE events.page = 'NextSong';
    """)

    # ----------------------------------------------------------------------
    # Dimension table: users
    # ----------------------------------------------------------------------
    user_table_insert = ("""
        INSERT INTO users (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )
        SELECT DISTINCT
            userid       AS user_id,
            firstname    AS first_name,
            lastname     AS last_name,
            gender,
            level
        FROM staging_events
        WHERE userid IS NOT NULL
          AND page = 'NextSong';
    """)

    # ----------------------------------------------------------------------
    # Dimension table: songs
    # ----------------------------------------------------------------------
    song_table_insert = ("""
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """)

    # ----------------------------------------------------------------------
    # Dimension table: artists
    # ----------------------------------------------------------------------
    artist_table_insert = ("""
        INSERT INTO artists (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
        SELECT DISTINCT
            artist_id,
            artist_name     AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """)

    # ----------------------------------------------------------------------
    # Dimension table: time
    # ----------------------------------------------------------------------
    time_table_insert = ("""
        INSERT INTO time (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT
            start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(weekday FROM start_time) AS weekday
        FROM songplays;
    """)
