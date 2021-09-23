#----------Create the database. You can skip this step if you want to read the file from an existing database--------

create database hive_youtube;

#-----------Switching to the destined database------------------------------------------------------------
use hive_youtube;

#------------Create the table youtube---------------------------
create table youtube(video_id string,uploader string,intervals int,category string,length_video int,views int,rating float,no_ratings int,no_comments int,rel_video_id string)row format delimited fields terminated by ',';

#------------Load the table youtube from the input file. The input file can be either be in the local file system or in HDFS.-------
load data local inpath '/home/hadoop/Learnbay/youtube' into table youtube;

#-------------Find out the top 5 categories with maximum number of videos uploaded.-----------
insert overwrite local directory '/home/hadoop/Learnbay/youtube_op_usecase1' select category, count (*) as video_count from youtube group by category sort by video_count desc limit 5;

#-------------Find out the most viewed videos
insert overwrite local directory '/home/hadoop/Learnbay/youtube_op_usecase2' select video_id, views from youtube order by views desc limit 1;

#-------------Find out the top 10 rated videos 
insert overwrite local directory '/home/hadoop/Learnbay/youtube_op_usecase3' select video_id,no_ratings from youtube order by no_ratings desc limit 10;


### Save this file with an extension as .q, which stands for query file
### you can execute this file, which inturn executes all the commands in it at one shot.
### hive -f <filename>.q
