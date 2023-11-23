create_staging_events_table_sql="""
CREATE TABLE IF NOT EXISTS staging_events (
   artist               varchar,
   auth                 varchar, 
   firstName            varchar,
   gender               varchar,
   iteminSession        varchar,
   lastName             varchar,   
   length               varchar,
   level                varchar,
   location             varchar,
   method               varchar,
   page                 varchar,
   registration         varchar,
   sessionid            int ,
   song                 varchar,
   status               int,
   ts                   bigint,  
   userAgent            varchar,
   userid               int
   )
"""

create_staging_songs_table_sql="""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     varchar,
    artist_longitude    varchar,
    artist_location     varchar(1024),
    artist_name         varchar(1024),
    song_id             varchar,
    title               varchar,
    duration            numeric(14,7),
    year                int      
   )
"""

create_fact_songplays_table_sql="""
CREATE TABLE IF NOT EXISTS fact_songplays(
    songplay_id          int identity(0,1) primary key , 
/*    songplay_id          varchar(32) NOT NULL, */
    start_time           timestamp,
    user_id              int,
    level                varchar,
    song_id              varchar NOT NULL,
    artist_id            varchar NOT NULL,
    session_id           int,
    location             varchar,
    user_agent           varchar  
    )
"""
create_dim_users_table_sql="""
CREATE TABLE IF NOT EXISTS dim_users(
   user_id               int NOT NULL primary key,
   first_name            varchar,
   last_name             varchar,
   gender                varchar,
   level                 varchar 
)
"""

create_dim_songs_table_sql="""
CREATE TABLE IF NOT EXISTS dim_songs(
   song_id               varchar NOT NULL primary key,
   title                 varchar,
   artist_id             varchar NOT NULL,
   year                  int,
   duration              varchar
)
"""

create_dim_artists_table_sql="""
CREATE TABLE IF NOT EXISTS dim_artists(
   artist_id             varchar NOT NULL primary key,
   name                  varchar(1024),
   location              varchar(1024),
   latitude              varchar,
   longitude             varchar 
)
"""

create_dim_time_table_sql="""
CREATE TABLE IF NOT EXISTS dim_time(
   start_time            timestamp NOT NULL primary key,
   hour                  int,
   day                   int,
   week                  int,
   month                 int,
   year                  int,
   weekday               varchar 
)
"""
