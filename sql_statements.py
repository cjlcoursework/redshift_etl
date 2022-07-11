DHW_SCHEMA = "data"
STAGING_SCHEMA = "stage"

STAGING_SONG_TABLE = f"{STAGING_SCHEMA}.songs"
STAGING_LOGS_TABLE = f"{STAGING_SCHEMA}.logs"

FACT_SONGPLAY_TABLE = f"{DHW_SCHEMA}.fact_songplays"
DIM_USER_TABLE = f"{DHW_SCHEMA}.dim_user"
DIM_SONG_TABLE = f"{DHW_SCHEMA}.dim_song"
DIM_ARTIST_TABLE = f"{DHW_SCHEMA}.dim_artist"
DIM_TIME_TABLE = f"{DHW_SCHEMA}.dim_time"

# STAGING
drop_stage_songs = f"DROP table if exists {STAGING_SONG_TABLE}"
drop_stage_logs = f"DROP table if exists {STAGING_LOGS_TABLE}"

create_stage_songs = (f"""
create table {STAGING_SONG_TABLE} (
  num_songs int,
  artist_id varchar(60),
  artist_latitude real,
  artist_longitude real,
  artist_location varchar(255),
  artist_name varchar(255),
  song_id varchar(60),
  title varchar(1024),
  duration numeric,
  year int  )
""")

create_stage_logs = (f"""
    create table {STAGING_LOGS_TABLE} (
      artist varchar(255),
      auth varchar(60),
      first_name varchar(255),
      gender varchar(2),
      item_in_session int,
      last_name varchar(255),
      length real,
      level varchar(60),
      location varchar(255),
      method varchar(5),
      page varchar(255),
      registration bigint,
      session_id int,
      song varchar(255),
      status int,
      ts bigint,
      user_agent varchar(2048),
      user_id varchar(60) )
      """)

#  ----- DWH -----------
songplay_table_drop = f"drop table if exists {FACT_SONGPLAY_TABLE}"
user_table_drop = f"drop table if exists {DIM_USER_TABLE}"
song_table_drop = f"drop table if exists {DIM_SONG_TABLE}"
artist_table_drop = f"drop table if exists {DIM_ARTIST_TABLE}"
time_table_drop = f"drop table if exists {DIM_TIME_TABLE}"

# CREATE TABLES
songplay_table_create = (f"""
create table {FACT_SONGPLAY_TABLE} (
    songplay_id bigint identity(1, 1) PRIMARY KEY distkey,
    time_id varchar(60) sortkey,
    start_ts timestamp,
    user_id int , 
    level text  , 
    song_id text, 
    artist_id text, 
    session_id int ,
    location text ,
    user_agent text 
    )""")

user_table_create = (f"""
create table {DIM_USER_TABLE}
(
    user_id    int PRIMARY KEY sortkey,
    first_name text,
    last_name  text,
    gender     varchar(2)  ,  
    level      text  
) diststyle all""")

song_table_create = (f"""
create table {DIM_SONG_TABLE} (
    song_id  varchar(60)  PRIMARY KEY  distkey,
    title text  ,
    artist_id text  ,
    year int  ,
    duration numeric  )  
""")

artist_table_create = (f"""
create table {DIM_ARTIST_TABLE} (
    artist_id  varchar(60)  PRIMARY KEY  distkey,
    name text  ,
    location text  ,
    latitude double precision,
    longitude  double precision) 
""")

time_table_create = (f"""
create table {DIM_TIME_TABLE} (
    time_id varchar(60) sortkey,
    hour int  , 
    day int , 
    week int , 
    month int , 
    year int , 
    weekday boolean  
)  diststyle all
""")

# BUILD STAR INSERTS

insert_dim_song = f"""
INSERT into {DIM_SONG_TABLE} (song_id, title, artist_id, year, duration)
    SELECT distinct song_id, title, artist_id, year, duration FROM {STAGING_SONG_TABLE} where song_id is not null"""

insert_dim_artist = f"""INSERT into {DIM_ARTIST_TABLE} (artist_id, name, location, latitude, longitude)
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM {STAGING_SONG_TABLE} where artist_id is not null"""

insert_dim_user = f"""INSERT into {DIM_USER_TABLE} (user_id, first_name, last_name, gender, level)
select distinct user_id::int, first_name, last_name, gender, level from {STAGING_LOGS_TABLE} where user_id is not null and len(user_id) > 0"""

insert_fact_songplay = f"""INSERT into {FACT_SONGPLAY_TABLE} (
  time_id,start_ts,user_id,level,song_id,artist_id,session_id,location,user_agent 
) 
with X as (select 
	timestamp 'epoch' + ts/1000 * interval '1 second' AS start_ts
    , user_id::int
    , level
    , nvl(S.song_id, null) as song_id -- on song == song
    , nvl(S.artist_id, null)  as artist_id-- on artistname == artist name
    , session_id
    , location
    , user_agent
from {STAGING_LOGS_TABLE} L
join {DIM_SONG_TABLE} S on S.title = L.song ) 
  select 
	to_char(start_ts, 'YYYYMMDDHH24MISS') as time_id
    , start_ts
    , user_id::int
    , level
    , song_id -- on song == song
    , artist_id-- on artistname == artist name
    , session_id
    , location
    , user_agent
from X"""

insert_dim_time = f"""insert into {DIM_TIME_TABLE} (
  time_id, year, month, day, week, hour, weekday
 )
select
  time_id,
  DATE_PART(year,start_ts) as year,
  DATE_PART(month,start_ts) as month,
  DATE_PART(day,start_ts) as day,
  DATE_PART(week,start_ts) as week,
  DATE_PART(hour,start_ts) as hour,
  case when DATE_PART(dayofweek,start_ts) in (0,6) then true else false end as is_weekday
from {FACT_SONGPLAY_TABLE}"""

drop_table_queries = [drop_stage_logs,
                      drop_stage_songs,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop
                      ]

create_table_queries = [
    songplay_table_create,
    user_table_create,
    artist_table_create,
    time_table_create,
    song_table_create,
    create_stage_songs,
    create_stage_logs]
