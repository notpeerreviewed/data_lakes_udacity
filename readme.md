## Data Lake Project - Data Engineering Nano Degree

### Files

#### dl.cfg
This file contains the AWS key and secret. You'll need to add your own credentials to this file before running the project.

#### etl.py
This is the primary file containing the code to read song and log data from the Udacity S3 bucket, and then to write the output files back to your own output folder. You should specify your own output directory for files. 

The files stored in the Udacity S3 bucket can be read by setting your input drive to "s3a://udacity-dend/song_data/\*/\*/\*/\*.json" however when you are testing the code it is highly recommended that you choose a subset of the data to save time. For example, 
"s3a://udacity-dend/song_data/A/A/\*/\*.json".


### Project instructions

The basic instructions for this project were to create a star schema optimised for queries on song play analysis.

We need to create several tables including songplays, users, songs, artists, and time.


### How to run this project

1. Make sure you have created a user in the IAM section of your AWS dashboard. The user needs to have programmatic access.
2. Take note of your access key and secret. Be careful to NOT INCLUDE the dl.cfg in any public commits to your Github account. I keep my credentials in a separate file and am careful to include that file explicitly in a .gitignore file when I create my repo so that it doesn't get inadvertently shared.
3. Once the code is written in the Udacity workspace you need to open a Terminal from the workspace launcher and type 'python etl.py'. This will run the etl.py script. When you are testing the script it is a good idea to use the workspace to save your output until you know the project is working correctly. Once you are sure everything is good, then you can change your output directly to your own S3 bucket in the Udacity area.