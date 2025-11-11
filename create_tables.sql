-- DROP TABLES

DROP TABLE IF EXISTS staging_events;
DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS songplays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS songs;
DROP TABLE IF EXISTS artists;
DROP TABLE IF EXISTS time;

-- CREATE TABLES

CREATE TABLE staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          CHAR(1),
    itemInSession   INT,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INT,
    song            VARCHAR,
    status          INT,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INT
);

CREATE TABLE staging_songs (
    num_songs          INT,
    artist_id          VARCHAR,
    artist_latitude    FLOAT,
    artist_longitude   FLOAT,
    artist_location    VARCHAR,
    artist_name        VARCHAR,
    song_id            VARCHAR,
    title              VARCHAR,
    duration           FLOAT,
    year               INT
);

CREATE TABLE songplays (
    playid      VARCHAR PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR,
    artist_id   VARCHAR,
    session_id  INT,
    location    VARCHAR,
    user_agent  VARCHAR
);

CREATE TABLE users (
    user_id     INT PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      CHAR(1),
    level       VARCHAR
);

CREATE TABLE songs (
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR,
    year        INT,
    duration    FLOAT
);

CREATE TABLE artists (
    artist_id   VARCHAR PRIMARY KEY,
    name        VARCHAR,
    location    VARCHAR,
    latitude    FLOAT,
    longitude   FLOAT
);

CREATE TABLE time (
    start_time  TIMESTAMP PRIMARY KEY,
    hour        INT,
    day         INT,
    week        INT,
    month       INT,
    year        INT,
    weekday     INT
);
