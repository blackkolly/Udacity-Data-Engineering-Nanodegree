# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# Create Tables

songplay_table_create = ("""
CREATE TABLE  songplays(
       songplay_id SERIAL Primary Key,
       start_time timestamp not Null,
       user_id int not Null,
       level text,
       song_id text,
       artist_id text,
       session_id text,
       location text,
       user_agent text
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id int Primary Key,
    first_name text,
    last_name text,
    gender text,
    level text
);
""")

song_table_create = ("""
CREATE TABLE songs(
   song_id text Primary Key,
   title text,
   artist_id text,
   year int,
   duration float
);
""")

artist_table_create = ("""
CREATE TABLE  artists (
   artist_id text Primary Key,
   name text,
   location text,
   latitude float,
   longitude float
);
""")

time_table_create = ("""
CREATE TABLE time (
   start_time timestamp Primary Key,
   hour int,
   day int,
   week int,
   month int,
   year int,
   weekday int
);
""")

# Insert Records

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id,
location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time, user_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO UPDATE SET level = excluded.level ;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING ;
""")
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, 
%s, %s)
ON CONFLICT (start_time)
    DO NOTHING ;
""")
# FIND SONGS

song_select = ("""SELECT song_id , artists.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id 
              where title=%s AND name=%s AND duration=%s""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]