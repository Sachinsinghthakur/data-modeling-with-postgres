#drop existing tables

drop_songs_tbl="DROP TABLE IF EXISTS songs"

drop_users_tbl="DROP TABLE IF EXISTS users"

drop_artists_tbl="DROP TABLE IF EXISTS artists"

drop_time_tbl = "DROP TABLE IF EXISTS time_table"

drop_songplays_tbl = "DROP TABLE IF EXISTS songplays"

drop_table_queries=[drop_artists_tbl,drop_songplays_tbl,drop_songs_tbl,drop_users_tbl,drop_time_tbl]

#create table queries

create_songs_tbl= ("""CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR NOT NULL PRIMARY KEY,
artist_id VARCHAR NOT NULL,
title VARCHAR NOT NULL,
year INT,
duration numeric
);""")

create_artists_tbl=("""CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC,
    location VARCHAR
    
    );""")

create_users_tbl = ("""CREATE TABLE IF NOT EXISTS users(
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    );""")

create_time_tbl = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

create_songplays_tbl = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar,
    user_agent varchar
);
""")

create_table_queries =[create_artists_tbl,create_songplays_tbl,create_songs_tbl,create_users_tbl,create_time_tbl]

insert_into_songs= ("""INSERT INTO songs (song_id,title,artist_id,year,duration)
                    values(%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING""")

insert_into_artists=("""INSERT INTO artists(artist_id,name,location,latitude,longitude)
                     values(%s,%s,%s,%s,%s) ON CONFLICT(artist_id) DO NOTHING""")       

insert_into_users = ("""INSERT INTO users (user_id,first_name,last_name,gender,level)
                     values (%s,%s,%s,%s,%s) ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.leveL""")   

insert_into_time =("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                   values(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(start_time) DO NOTHING""")

insert_into_songplays=("""INSERT INTO songplays(songplay_id,song_id,artist_id,level,start_time,user_id,session_id,location,user_agent) 
                       values(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(songplay_id) DO NOTHING""")    

select_song_artist_id = ("""SELECT song_id,songs.artist_id FROM songs JOIN artists
                        ON songs.artist_id=artists.artist_id 
                        WHERE title = %s AND name = %s AND duration= %s""")

