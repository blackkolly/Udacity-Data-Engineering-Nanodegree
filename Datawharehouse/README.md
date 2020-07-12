# Data Warehouse

*Udacity Project for Data Engineer Nanodegree*

##  Project Summary
***
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs 
on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
### Purpose
***
 To build an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables
which to be used for analytic to get insight.

# Database Schema

*The schema used for this project is the star schema.The reason for using this schema is to model this dataset by dividing them into facts and dimensions in a well structured manner.
*There is one main fact table containing all the measures associated to each event ( songplays), and 4 dimentional tables, each with a primary key that is being referenced from the fact table.
   *Fact Table
   *Fact table consists "songplays" table which contains the metadata of the complete information about each user activity. 

   *Dimension Tables
   *It consist the users,songs,artists and time table" are going to be dimension tables. 


# Procedure followed

The following steps must be taken after the other when to create the database tables and run the ETL pipeline.
Ensure the AWS Redshift cluster up and running and enter the login details in the dwh.cfg file. 

Run the create_tables script to set up the database staging and analytical tables:
```bash
python3 create_tables.py
```
Run the ETL pipeline to extract the data from S3 to Redshift and load it into the target table.
```bash
python3 etl.py
```

# Project Files

* **[create_tables.py](create_tables.py)**: Script will drop old tables (if exist) ad re-create new tables.
* **[sql_queries.py](sql_queries.py)**: This file is made of SQL statement that are used to create_tables.py ,etl.py and analytics.py.
* **[etl.py](etl.py)**: This file is used to extract information from S3 into staging tables on Redshift and 
                        then processed into the analytics tables on Redshift



# Dataset used
  
  Song Dataset
  Log Dataset 

 
 

