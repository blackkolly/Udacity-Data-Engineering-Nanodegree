# Data Lake

*Udacity Project for Data Engineer Nanodegree*

##  Project Summary
***
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Purpose
***
 To build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables which to be used by analytic team to get insight.

# Database Schema

*The schema used for this project is the star schema.The reason for using this schema is to model this dataset by dividing them into facts and dimensions in a well structured manner.
*There is one main fact table containing all the measures associated to each event ( songplays), and 4 dimentional tables, each with a primary key that is being referenced from the fact table.
   *Fact Table
   *Fact table consists "songplays" table which contains the metadata of the complete information about each user activity. 

   *Dimension Tables
   *It consist the users,songs,artists and time table" are going to be dimension tables. 


# Procedure followed

The following steps must be taken after the other when to read data from s3 and write them back to s3 using spark.
Ensure you enter AWS credentials login details in the dl.cfg file. 

Run the ETL pipeline to extract the data from S3 processes that data using Spark, and writes them back to S3
```bash
python3 etl.py
```

# Project Files

* **[dl.cfg ](dl.cfg )**: configuration file.

* **[etl.py](etl.py)**: This file is used to extract information from S3 processes that data using Spark, and writes them back                           to S3.
                       



# Dataset used
  
  Song Dataset
  Log Dataset 

 
 

