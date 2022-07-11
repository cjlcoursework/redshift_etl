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

---
##### <font color='green'>ETL Requirement: </font>
- COPY data from the `song_data` into a `stage.songs` staging table
- COPY data from the `log_data` into a `stage.logs` staging table
- INSERT into a Star schema containing the following tables:
    - `fact_songplays` --  events where users have 'listened' to a song
    - `dim_song` -- information about each song, such as title, duration, etc.
    - `dim_user` -- a user is the persona that listened to each song
    - `dim_artist`  --- an artist is the person who created each song
    - `dim_time` -- the time dimension just breaks out the timestamp of each event into data parts
---
##### <font color='green'>The overall flow is:</font>
* Initialize the Redshift database:
    - create a Redshift cluster
    - create a STAGE schema and a DATA schema
* create the songs and logs table in the STAGE schema
* create the star schema in the DATA schema
* insert data from the stage tables into the Star schema
---
##### <font color='red'>Important</font>
* This program creates the cluster and loads the data, all in one
* This program normally deletes the cluster after running
* To keep the cluster set the etl.main() parameter to `drop_cluster=False`
  ```
  if __name__ == '__main__':
      main(drop_cluster=True)
  ```
* Loading the songs_data takes time - it feels ok for an assignment, but in production we'd want to play with this

---
##### <font color='yellow'>Run Log:</font>
Example of running python3 etl.py:
```
/usr/bin/python3 /Users/christopherlomeli/Source/courses/udacity/data-engineer/redshift_etl/etl.py
role does not exist
1.1 Creating a new IAM Role
1.2 Attaching Policy
1.3 Get the IAM role ARN
Cluster dwhCluster is being created ...
ec2.SecurityGroup(id='sg-0ea22cb2a9f28e4e9')
Cluster dwhCluster is being created ...
waiting 30 sec for cluster....
Cluster dwhCluster is being created ...
waiting 30 sec for cluster....
Cluster dwhCluster is being created ...
waiting 30 sec for cluster....
Cluster dwhCluster is being created ...
waiting 30 sec for cluster....
Cluster dwhCluster is being created ...
waiting 30 sec for cluster....

Cluster dwhCluster is up ...
drop tables ...
create tables ...
Copying stage.logs from s3 ....  this could take several minutes ...
Completed stage.logs took 4.304821968078613 milliseconds ...
-------------------

Copying stage.songs from s3 ....  this could take several minutes ...
Completed stage.songs took 273.2737216949463 milliseconds ...
-------------------

Load data.dim_song ....
Load data.dim_artist ....
Load data.dim_user ....
Load data.fact_songplays ....
Load data.dim_time ....
Done

query tables ...

----------------
select count(*) from data.fact_songplays limit 5
----------------
[1144]

----------------
select count(*)  from data.dim_user limit 5
----------------
[105]

----------------
select count(*)  from data.dim_song limit 5
----------------
[14896]

----------------
select count(*)  from data.dim_artist limit 5
----------------
[10025]

----------------
select count(*)  from data.dim_time limit 5
----------------
[1144]

----------------
select * from data.fact_songplays limit 5
----------------
[254, '20181102103311', datetime.datetime(2018, 11, 2, 10, 33, 11), 15, 'paid', 'SOXDCJM12AB0183422', 'AR2RFZP1187FB4B33E', 172, 'Chicago-Naperville-Elgin, IL-IN-WI', '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"']
[744, '20181103190133', datetime.datetime(2018, 11, 3, 19, 1, 33), 95, 'paid', 'SOPGUJB12A670212C4', 'ARXGZ2L1187B9B687D', 152, 'Winston-Salem, NC', '"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"']
[840, '20181104065112', datetime.datetime(2018, 11, 4, 6, 51, 12), 25, 'paid', 'SORKKTY12A8C132F3E', 'ARIH5GU1187FB4C958', 128, 'Marinette, WI-MI', '"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"']
[124, '20181104074524', datetime.datetime(2018, 11, 4, 7, 45, 24), 25, 'paid', 'SORKXUL12AB01821DA', 'ARX1P2N1187FB59127', 128, 'Marinette, WI-MI', '"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"']
[950, '20181104074524', datetime.datetime(2018, 11, 4, 7, 45, 24), 25, 'paid', 'SOBEUMD12AB018A9BC', 'ARU9OUL1187FB39BC1', 128, 'Marinette, WI-MI', '"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.143 Safari/537.36"']

----------------
select * from data.dim_user limit 5
----------------
[2, 'Jizelle', 'Benjamin', 'F', 'free']
[3, 'Isaac', 'Valdez', 'M', 'free']
[4, 'Alivia', 'Terrell', 'F', 'free']
[5, 'Elijah', 'Davis', 'M', 'free']
[6, 'Cecilia', 'Owens', 'F', 'free']

----------------
select * from data.dim_song limit 5
----------------
['SOWFLKH12A8AE4651B', 'Hold It Up', 'ARIIFAI1187B9B101B', 2007, Decimal('190')]
['SOQATZO12AB0187A96', 'Water Tapestry', 'ARYU4I01187FB5B8B8', 1998, Decimal('175')]
['SOIAQCE12AAF3B562B', 'Laser Light', 'ARZD4UW1187B9AB3D2', 2000, Decimal('401')]
['SOFLRZW12A6D4F9E75', 'Ring the Bell', 'ARY409C1187B9B723A', 2005, Decimal('361')]
['SOAJLUX12AB0188F4D', 'You Take It All', 'AR4VT3E1187FB570BB', 2001, Decimal('293')]

----------------
select * from data.dim_artist limit 5
----------------
['ARHY77F1187B9B5CFF', "Poverty's no Crime", '', None, None]
['ARVEJ9M1187FB4DC44', 'Les Ogres De Barback', '', None, None]
['AR9JET41187FB3DE77', 'Goldie', 'MORRISTON, Florida', 29.28437042236328, -82.44532012939453]
['AR78HH21187B9B84D1', 'Nicolas Repac', '', None, None]
['ARTEOIV11C8A417A4E', 'Mala Reputación', '', None, None]

----------------
select * from data.dim_time limit 5
----------------
['20181101211113', 21, 1, 44, 11, 2018, False]
['20181102091232', 9, 2, 44, 11, 2018, False]
['20181102091337', 9, 2, 44, 11, 2018, False]
['20181102091915', 9, 2, 44, 11, 2018, False]
['20181102102125', 10, 2, 44, 11, 2018, False]

----------------
select distinct  F.songplay_id, U.user_id, S.song_id, A.artist_id, T.time_id
from data.fact_songplays F
join data.dim_user U on U.user_id = F.user_id
join data.dim_song S on S.song_id = F.song_id
join data.dim_artist A on A.artist_id = F.artist_id
join data.dim_time T on T.time_id = F.time_id
limit 5
    
----------------
[2, 15, 'SORLLRN12A58A7F1EE', 'AR65K7A1187FB4DAA4', '20181121141932']
[10, 49, 'SODAUOI12AB0181942', 'AR1TJ841187B9912D3', '20181130182808']
[26, 30, 'SOJRMKC12A8C13AD5E', 'AR7FMHB1187FB443E9', '20181130061828']
[18, 80, 'SOJRMKC12A8C13AD5E', 'AR7FMHB1187FB443E9', '20181114185035']
[34, 24, 'SOVEBYA12A6D4F413A', 'AR4VT3E1187FB570BB', '20181119072936']
Cluster dwhCluster is up ...
Bring cluster down....

Process finished with exit code 0


```
