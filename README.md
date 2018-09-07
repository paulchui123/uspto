# **USPTO PATENT DATA PARSER**

Author: Joseph Lee

Email: joseph@ripplesoftware.ca

## **Description:**
This python script is based on a script from University of Illinois (http://abel.lis.illinois.edu/UPDC/Downloads.html).
Several parts of the script have been improved to increase the data integrity and memory utilization of the
original script.  The whole packages is therefore contained into one file which can be run from the command line.
The usage of the script is outlined below:

## **Instructions:**
There are three steps.
1. Install the database
2. Run the parser
3. Schedule the updater

### 1. Install the database

Run the appropriate database creation scripts depending if you intend to store the USPTO data in MySQL or PostgreSQL.  The script will create a user and limit the scope of the user to the uspto database. If you want to change the default password for the user that the script will create, edit the .sql file before running it.

_MySQL_

installation/uspto_create_database_mysql.sql

_PostgreSQL_

installation/uspto_create_database_postgresql.sql

### 2. Run the parser

First, the auth credentials for the database must be added to the file USPTOParser.py if database storage will be specified. Text search for the phrase "# Database args" to find the location where database credentials must be changed. Enter "mysql" or "postgresql" as the database_type. Enter the port of your MySQL or PostgreSQL installation if you have a non-default port. If you changed the default password in the database creation file, then you should also change the password here.

Secondly, you can set the number of threads with a command line argument '-t [int]' where [int] is a number between 1 and 20.  If you do not specify the number of threads, then the default of 10 threads will be used.

Finally, you must specify the location for the data to be stored.  These options are: '-csv' and '-database'.  You must include at least one. These arguemnts tell the script where you want the data to be stored. The following example is the command to store in csv file and database with 20 process threads.  You should set the 'database_insert_mode' to specify whether you want the data to be inserted into the database after each data object is found and parsed (`each`), or in bulk post parsing of each file (`bulk`).  `bulk` setting improve database transactions per second.

$ python USPTOParser.py -csv -database -t 20

### 3. Schedule the updater

The script will also run in update mode. This is done by passing the '-update' argument when running the script.
The script will then know to check your previous data destination(s) and continue to look for new patent data
release files that have been published on the USPTO website (https://bulkdata.uspto.gov/).  The new files are then
parsed and stored in the destinations you previously specified.  Since database data files are released every
week, the updater can be scheduled once a week to keep your data up-to-date.  Also, you can reduce the number of threads for the updater script to not disrupt the user experience during update time.

$ python USPTOParser.py -update -t 3

## **Further Information:**

### CPU Load balancing

The script currently uses a load balancer which initially calculates the number of CPU cores in your computer and then will check periodically how much CPU load is being consumed.  If the overall CPU load is less than 75%, another thread is added.  These settings can be adjusted in the script.

### Bulk Database Insertion Performance

The option to insert each document's data into the database can be done two ways.  The script can insert each document record immediately after it is parsed or in bulk after a file is finished being parsed.  Using bulk storage utilizes .csv files to temporarily store the data before it is inserted in bulk.  If the `-csv` command line argument is not set, then the .csv. files are erased after being used to load the data.  

Performance of using bulk uploader resulted in... ??? 
