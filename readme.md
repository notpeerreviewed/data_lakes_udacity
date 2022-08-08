## Data Lake Project - Data Engineering Nano Degree

### Notes

1. Make sure you have created a user through the IAM Management console with the appropriate user permissions
2. Record your AWS credentials in a configuration file. These will be imported to environment variables.
3. This project did not include an EC2 key pair.

### Project instructions

The basic instructions for this project were to create a star schema optimised for queries on song play analysis.

We need to create several tables as follows:

Fact table
    songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, lattitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday


### How to run this project

1. Make sure you have created a user