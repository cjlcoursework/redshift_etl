# redshift_etl

### This project contains
- `etl.py` -- main program for creating and loading our Redshift cluster
- `create_tables.py` -- all cluster, schema, and table DDL
- `sql_statements.py` -- all SQL strings 
- `queries.py` -- perform a few sanity queries 
- `dwh.cfg`  -- database configurations

---
### Overview
##### <font color='green'>Load log and song tables from a public s3 folders directly into two STAGING tables</font>
- configuration data is in the `dwh.cfg` file in this project
- The `song_data` in s3 is found in configuration(S3.SONG_DATA)
- The `log_data` in s3 is found in configuration(S3.LOG_DATA)

##### <font color='green'>ETL Requirement: </font>
- COPY data from the `song_data` into a `stage.songs` staging table
- COPY data from the `log_data` into a `stage.logs` staging table
- INSERT into a Star schema containing the following tables:
    - `fact_songplays` --  events where users have 'listened' to a song
    - `dim_song` -- information about each song, such as title, duration, etc.
    - `dim_user` -- a user is the persona that listened to each song
    - `dim_artist`  --- an artist is the person who created each song
    - `dim_time` -- the time dimension just breaks out the timestamp of each event into data parts

##### <font color='green'>The overall flow is:</font>
* Initialize the Redshift database:
    - create a Redshift cluster
    - create a STAGE schema and a DATA schema
* create the songs and logs table in the STAGE schema
* create the star schema in the DATA schema
* insert data from the stage tables into the Star schema

##### <font color='red'>Important</font>
* This program creates the cluster and loads the data, all in one
* This program normally deletes the cluster after running
* To keep the cluster set the etl.main() parameter to `drop_cluster=False`



