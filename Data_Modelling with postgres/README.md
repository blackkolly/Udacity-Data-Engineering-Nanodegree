# Data Modeling with Postgres for Sparkifydb

*Udacity Project for Data Engineer Nanodegree*

##  Project Summary
***
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Your role is to create a database schema and ETL pipeline for this analysis.


### Purpose
***
The purpose of this project is to create a Postgres database and ETL pipeline to optimize queries on song play analysis to help Sparkify's analytics team. 

# Database Schema

*The schema used for this project is the star schema.The reason for using this schema is to model this dataset by dividing them into facts and dimensions in a well structured manner.
*There is one main fact table containing all the measures associated to each event (user song plays), and 4 dimentional tables, each with a primary key that is being referenced from the fact table.
   *Fact Table
   *Fact table consists "songplays" table which contains the metadata of the complete information about each user activity. 

   *Dimension Tables
   *It consist the users,songs,artists and time table" are going to be dimension tables. 


# Procedure followed

The following steps must be taken after the other when to create the database tables and run the ETL pipeline.

To create tables:
```bash
python3 create_tables.py
```
Inset values into the tables via ETL:
```bash
python3 etl.py
```

# Project Files


* **[data](data)**: Folder containing data of songs and logs 
* **[create_tables.py](create_tables.py)**: Python script to create the schema structure into the database.
* **[sql_queries.py](sql_queries.py)**: This file is used by create_tables.py and etl.py throughout the ETL process
* **[etl.py](etl.py)**: This file is used to extract information from Song and Log data folder and inserting them database tables
* **[etl.ipynb](etl.ipynb)**:The python notebook that was written to develop the logic behind the etl.py process
* **[test.ipynb](test.ipynb)**:This notebook was used to certify if our ETL process was being successful or not


# Dataset used
  
  Song Dataset - song metadata.
  Log Dataset - User activity log.

 
 

