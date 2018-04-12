**USPTO PATENT DATA PARSER**

Author: Joseph Lee
Email: joseph.lee.esl@gmail.com

Description:
------------
This python script is based on a script from University of Illinois (http://abel.lis.illinois.edu/UPDC/Downloads.html).
Several parts of the script have been improved to increase the data integrity and memory utilization of the
original script.  The whole packages is therefore contained into one file which can be run from the command line.
The usage of the script is outlined below:

Instructions:
-------------
There are only three steps.  They are outlined below.

(1) - Run the database creation scripts if you intend to store the USPTO data in MySQL or PostgreSQL.  If you
want to change the default password for the user that the script will create, then edit the .sql file before running it.

MySQL
-----
installation/uspto_create_database_mysql.sql

PostgreSQL
----------
installation/uspto_create_database_postgresql.sql

(2) - Run the parser.

First, the auth credentials for the specified database must be added to the file USPTOParser.py if database
storage will be specified.  Text search for the phrase "# Database args" to find the location where database
credentials must be added.  Enter "mysql" or "postgresql" as the database_type.  Enter the port of your MySQL
or PostgreSQL installation.  If you changed the default password in the database creation file, then you should
also change the password here.  All other settings should be ok.

Secondly, you can set the number of threads by searching "number_of_threads =".  It's recommended to use between 3-6
threads.  

The parser accepts maximum two command line arguments when run for the first time.  These arguments are:
-csv and -database.  You must include at least one.  These  arguemnts tell the script where you want the data
to be stored. If you wanted the data stored in csv and database the command would be as follows.

NOTE: The csv storage is not functional yet.

$ python USPTOParser.py -csv -database

(3) - Cron the updater.

The script will also run in update mode.  This is done by passing the -update argument when running the script.
The script will then know to check your previous data destination(s) and continue to look for new patent data
release files that have been published on the USPTO website (https://bulkdata.uspto.gov/).  The new files are then
parsed and stored in the destinations you previously specified.  Since database data files are released every
week, the updater can be scheduled once a week to keep your data up-to-date.

$ python USPTOParser.py -update
