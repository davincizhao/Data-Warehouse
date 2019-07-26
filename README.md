## Project Data Warehouse
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


### Project Description
In this project, I'll apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 

So as a data engineer , My task is:
- Building an ETL pipeline that extracts the data from S3, 
- Staging Data in Redshift, and transforms data into a set of dimensional tables for their analytics team 
- Finding insights in what songs their users are listening to. 



## Database schema:
1.Fact table: songplays  ( songplay_id,start_time , user_id , level, song_id , artist_id , session_id , location , user_agent)

2.Dimensional tables:

- Table: songs (song_id , title , artist_id , year , duration )

- Table: artists (artist_id , name , location , latitude , longitude ) 

- Table: users (user_id , first_name , last_name , gender , level )

- Table time (start_time , hour , day , week , month , year , weekday ) 

3.Staging table:

- staging_events_table (artist varchar(255) encode text255,
                      auth varchar(255) encode text255,
                      firstName varchar(100),
                      gender varchar(1),
                      itemInSession integer,
                      lastName varchar(100),
                      length DOUBLE PRECISION,
                      level varchar(20),
                      location varchar(255) encode text255,
                      method varchar(10),
                      page varchar(50),
                      registration varchar(100),
                      sessionId integer,
                      song varchar(200),
                      status integer,
                      ts bigint,
                      userAgent varchar(255) encode text255,
                      userId integer)
                      diststyle even;
                                    
- staging_songs_table (num_songs int NOT NULL, 
                                artist_id varchar , 
                                latitude float, 
                                longitude float,
                                location varchar(255) encode text255,
                                artist_name varchar,
                                song_id varchar PRIMARY KEY NOT NULL,
                                title varchar ,
                                duration float,
                                year int);                                    
 
## ETL pipeline:

1.Create a RedShift Cluster on AWS,connect to the cluster.
2.Create fact table,dimension table and staging table for loading big data
(Song data: s3://udacity-dend/song_data ,Log data json path: s3://udacity-dend/log_json_path.json
and Log data: s3://udacity-dend/log_data).
3.Because there are very large data in S3,So use "copy" command to faster loading data from S3 to staging table.
4.Insert the values which select from staging table into the fact table and all dimension table .




## How to run python script.
- Environment requirement: AWS,IAM,Redshift. Assume Redshift Cluster is ok to connect from jupyter notebook.

- 1, collect all Redshift cluster's ID(host), database name, database user name,database password and database port 

- 2, After redshift's connection is ok, open the terminal , and input command "python create_tables.py" to create all the tables, 

- 3, run "python etl.py" to Extract data from S3 and insert into the tables on redshift cluster.

- 4, open new jupyter notebook "Finding_insights.ipynb" to test thedatabase and ETL pipeline

## Finding insights example
what songs their users are listening to.
![DWH1!](./dwh1.png "dwh1")
![DWH3!](./DWH3.png "dwh3")
