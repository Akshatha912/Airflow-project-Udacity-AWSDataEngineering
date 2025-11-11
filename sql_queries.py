# plugins/helpers/sql_queries.py
INSERT INTO public.users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
userid::INT AS user_id,
firstname AS first_name,
lastname AS last_name,
gender AS gender,
level AS level
FROM public.staging_events
WHERE userid IS NOT NULL;
""")


songs_table_insert = ("""
INSERT INTO public.songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id AS song_id,
title AS title,
artist_id AS artist_id,
year AS year,
duration AS duration
FROM public.staging_songs
WHERE song_id IS NOT NULL;
""")


artists_table_insert = ("""
INSERT INTO public.artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id AS artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS latitude,
artist_longitude AS longitude
FROM public.staging_songs
WHERE artist_id IS NOT NULL;
""")


# time table population uses songplays.start_time as source for consistent timestamps
time_table_insert = ("""
INSERT INTO public.time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
sp.start_time AS start_time,
EXTRACT(hour FROM sp.start_time)::INT AS hour,
EXTRACT(day FROM sp.start_time)::INT AS day,
EXTRACT(week FROM sp.start_time)::INT AS week,
EXTRACT(month FROM sp.start_time)::INT AS month,
EXTRACT(year FROM sp.start_time)::INT AS year,
EXTRACT(dow FROM sp.start_time)::INT AS weekday
FROM public.songplays sp
WHERE sp.start_time IS NOT NULL;
""")
