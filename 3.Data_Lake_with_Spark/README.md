# Project Overview
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Purspose of this project: create a database schema and ETL pipeline for this analysis.

## About this database
We have 1 fact table and 4 dimension table. 

***Fact Table***
- songplays: records in log data associated with song plays i.e. records with page NextSong
(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

***Dimension Tables***
- users: users in the app
(user_id, first_name, last_name, gender, level)

- songs: songs in music database
(song_id, title, artist_id, year, duration)

- artists: artists in music database
(artist_id, name, location, latitude, longitude)

- time: timestamps of records in songplays broken down into specific units
(start_time, hour, day, week, month, year, weekday)

You may use this schema to store data with lowest memory storage and high consistence between tables. They can use information in fact table and dimension table to find out which song has been sell, who bought it, statistic buyer by gender/level, which time it has been bought, information about the artist,... 

## How to run? 

Run this in the terminal

`python3 etl.py`

or you can use this in jupyter notebook

`! python3 etl.py`

## Why this schema and pipeline? 
- First of all it pass all the test form he Sparkify analytics team
- With this schema, we can store data with lowest memory storage and high consistence between tables
- Easy to query information from dimention table and fact table with least query. 
- The pipeline is automate and scalable. We can work with any bigger data set in the future. 
- The pipeline help us eliminate manual works. 