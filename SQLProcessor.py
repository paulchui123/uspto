# Used to operate the database.
# Remind to replace the arguments in the following funciton '__init__' with your own database parameters

import MySQLdb
import psycopg2
import traceback
import time
import sys
import os
from pprint import pprint

class SQLProcess:

    # TODO: write the script to accept a database password from stdin
    def __init__(self, database_args):

        # Pass the database type to class variable
        self.database_type = database_args['database_type']

        # Define class variables
        self._host = database_args['host']
        self._port = database_args['port']
        self._username = database_args['user']
        self._password = database_args['passwd']
        self._dbname = database_args['db']
        self._charset = database_args['charset']
        self._conn = None
        self._cursor = None

    # Load the insert query into the database
    def load(self, sql, args_array, logger):

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Execute the query passed into funtion
        try:
            self._cursor.execute(sql)
            #self._conn.commit()
            #result = self._cursor.fetchall()  #fetchone(), fetchmany(n)
            #return result  #return affected rows
        except Exception as e:
            # If there is an error and using databse postgresql
            # Then rollback the commit??
            if self.database_type == "postgresql":
                self._conn.rollback()

            # Print and log general fail comment
            print "Database INSERT query failed... " + args_array['file_name'] + " into table: " + args_array['table_name'] + " Document ID Number " + args_array['document_id']
            logger.error("Database INSERT query failed..." + args_array['file_name'] + " into table: " + args_array['table_name'] + " Document ID Number " + args_array['document_id'])
            print "Query string: " + sql
            logger.error("Query string: " + sql)
            # Print traceback
            traceback.print_exc()
            # Print exception information to file
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # This function accepts an array of csv files which need to be inserted
    # using COPY command in postgresql and ?? in MySQL
    def load_csv_bulk_data(self, args_array, logger):

        # Print message to stdout and log starting of bulk upload
        print '[Staring to load csv files in bulk to ' + args_array['database_type'] + ']'
        logger.info('[Staring to load csv files in bulk to ' + args_array['database_type'] + ']')

        # Set the start time
        start_time = time.time()

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Log the contents of the args_array['csv_file_array']
        #logger.warning(str(args_array['csv_file_array'])

        # Loop through each csv file and bulk copy into database
        for key, csv_file in args_array['csv_file_array'].items():

            if "table_name" in csv_file:
                # Print message to stdout and log about which table is being inserted
                print "Database bulk load query started for: " + key + " from filename: " + csv_file['csv_file_name']
                logger.info("Database bulk load query started for: " + key + " from filename: " + csv_file['csv_file_name'])

                # If postgresql build query
                if self.database_type == "postgresql":

                    try:
                        sql = "COPY " + csv_file['table_name'] + " FROM STDIN DELIMITER '|' CSV HEADER"
                        #self._cursor.copy_from(open(csv_file['csv_file_name'], "r"), csv_file['table_name'], sep = ",", null = "")
                        self._cursor.copy_expert(sql, open(csv_file['csv_file_name'], "r"))
                        # Return a successfull insertion flag
                        return True

                    except Exception as e:
                        # Roll back the transaction
                        self._conn.rollback()
                        # Print and log general fail comment
                        print "Database bulk load query failed... " + csv_file['csv_file_name'] + " into table: " + csv_file['table_name']
                        logger.error("Database bulk load query failed..." + csv_file['csv_file_name'] + " into table: " + csv_file['table_name'])
                        print "Query string: " + sql
                        logger.error("Query string: " + sql)
                        # Print traceback
                        traceback.print_exc()
                        # Print exception information to file
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # Return a unsucessful flag
                        return False

                # If MySQL build query
                elif self.database_type == "mysql":

                    # Set flag to determine if the query was successful
                    bulk_insert_successful = False
                    bulk_insert_failed_attempts = 1
                    # Loop until the file was successfully deleted
                    # NOTE : Used because MySQL has table lock errors
                    while bulk_insert_successful == False and bulk_insert_failed_attempts <= 10:

                        try:
                            # TODO: consider "SET foreign_key_checks = 0" to ignore
                            # LOCAL is used to set duplicate key to warning instead of error
                            # IGNORE is also used to ignore rows that violate duplicate unique key constraints
                            bulk_insert_sql = "LOAD DATA LOCAL INFILE '" + csv_file['csv_file_name'] + "' INTO TABLE " + csv_file['table_name'] + " FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES"
                            # Execute the query built above
                            self._cursor.execute(bulk_insert_sql)
                            # Return a successfull insertion flag
                            bulk_insert_successful = True

                        except Exception as e:

                            # Increment the failed counter
                            bulk_insert_failed_attempts += 1
                            # Print and log general fail comment
                            print "Database bulk load query failed... " + csv_file['csv_file_name'] + " into table: " + csv_file['table_name']
                            logger.error("Database bulk load query failed..." + csv_file['csv_file_name'] + " into table: " + csv_file['table_name'])
                            print "Query string: " + bulk_insert_sql
                            logger.error("Query string: " + bulk_insert_sql)
                            # Print traceback
                            traceback.print_exc()
                            # Print exception information to file
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                            # Return a unsucessful flag
                            if bulk_insert_failed_attempts > 9:
                                return False

        # Return a successfull message from the database query insert.
        return True

    # Used to retrieve ID by matching fields of values
    def query(self,sql):
        #try:
        if self._conn == None:
            self.connect()
            self._cursor.execute(sql)
            #self._conn.commit()
            result=self._cursor.fetchone()
            return int(result[0])
        else:
            self._cursor.execute(sql)
            #self._conn.commit()
            result=self._cursor.fetchone()
            return int(result[0])
        #finally:
            #self.close()

    # Used to remove records from database when a file previously
    # started being processed and did not finish. (when insert duplicate ID error happens)
    def remove_previous_file_records(self, call_type, file_name, logger):

        # Set process time
        start_time = time.time()

        # Print and log starting to check for previous attempt to process file
        print "[Checking database for previous attempt to process the " + call_type + " file: " + file_name + "...]"
        logger.info("[Checking database for previous attempt to process the " + call_type + " file:" + file_name + "...]")

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Set the table_name
        table_name = "STARTED_FILES"

        # Build query to check the STARTED_FILES table to see if this file has been started already.
        check_file_started_sql = "SELECT COUNT(*) as count FROM uspto." + table_name + " WHERE FileName = '" + file_name + "' LIMIT 1"

        # Execute the query to check if file has been stared before
        try:
            self._cursor.execute(check_file_started_sql)
            # Check the count is true or false.
            check_file_started = self._cursor.fetchone()
            #pprint(check_file_started[0]['count'])
            #print check_file_started

        except Exception as e:
            # Set the variable and automatically check if database records exist
            check_file_started = True
            # If there is an error and using databse postgresql
            # Then rollback the commit??
            if self.database_type == "postgresql":
                self._conn.rollback()

            # Print and log general fail comment
            print "Database check if " + call_type + " file started failed... " + file_name + " from table: uspto.STARTED_FILES"
            logger.error("Database check if " + call_type + " file started failed... " + file_name + " from table: uspto.STARTED_FILES")
            # Print traceback
            traceback.print_exc()
            # Print exception information to file
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

        # If the file has not been started processing yet
        if check_file_started[0] == 0:
            # Insert the file_name into the table keeping track of STARTED_FILES
            if self.database_type == "postgresql":
                insert_file_started_sql = "INSERT INTO uspto." + table_name + "  (FileName) VALUES($$" + file_name + "$$)"
            elif self.database_type == "mysql":
                insert_file_started_sql = "INSERT INTO uspto." + table_name + " (FileName) VALUES('" + file_name + "')"

            # Print and log not found previous attempt to process file
            print "No previous attempt found to process the " + call_type + " file: " + file_name + " in table: uspto.STARTED_FILES"
            logger.info("No previous attempt found to process the " + call_type + " file:" + file_name + " in table: uspto.STARTED_FILES")

            # Insert the record into the database that the file has been started.
            try:
                self._cursor.execute(insert_file_started_sql)

            except Exception as e:
                # If there is an error and using databse postgresql
                # Then rollback the commit??
                if self.database_type == "postgresql":
                    self._conn.rollback()

                # Print and log general fail comment
                print "Database insert " + call_type + " file started failed... " + file_name + " into table: uspto.STARTED_FILES"
                logger.error("Database insert " + call_type + " file started failed... " + file_name + " into table: uspto.STARTED_FILES")
                # Print traceback
                traceback.print_exc()
                # Print exception information to file
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


        # If the file was found in the STARTED_FILES table, delete all the records of that file in all tables.
        elif check_file_started[0] != 0:

            # Print and log found previous attempt to process file
            print "Found previous attempt to process the " + call_type + " file: " + file_name + " in table: uspto.STARTED_FILES"
            logger.info("Found previous attempt to process the " + call_type + " file:" + file_name + " in table: uspto.STARTED_FILES")

            # Build array to hold all table names to have
            # records deleted for patent grants
            if call_type == "grant":
                table_name_array = [
                    "GRANT",
                    "INTCLASS_G",
                    "CPCCLASS_G",
                    "USCLASS_G",
                    "INVENTOR_G",
                    "AGENT_G",
                    "ASSIGNEE_G",
                    "APPLICANT_G",
                    "NONPATCIT_G",
                    "EXAMINER_G",
                    "GRACIT_G",
                    "FORPATCIT_G"
                ]
            # Records deleted for patent applications
            elif call_type == "application":
                table_name_array = [
                    "APPLICATION",
                    "INTCLASS_A",
                    "USCLASS_A",
                    "CPCCLASS_A",
                    "FOREIGNPRIORITY_A",
                    "AGENT_A",
                    "ASSIGNEE_A",
                    "INVENTOR_A",
                    "APPLICANT_A"
                ]

            # Loop through each table_name defined by call_type
            for table_name in table_name_array:

                # Build the SQL query here
                remove_previous_record_sql = "DELETE FROM uspto." + table_name + " WHERE FileName = '" + file_name + "'"

                # Set flag to determine if the query was successful
                records_deleted = False
                records_deleted_failed_attempts = 1
                # Loop until the file was successfully deleted
                # NOTE : Used because MySQL has table lock errors
                while records_deleted == False and records_deleted_failed_attempts < 10:
                    # Execute the query pass into funtion
                    try:
                        self._cursor.execute(remove_previous_record_sql)
                        records_deleted = True
                        #TODO: check the numer of records deleted from each table and log/print
                        # Print and log finished check for previous attempt to process file
                        print "Finished database delete of previous attempt to process the " + call_type + " file: " + file_name + " table: " + table_name
                        logger.info("Finished database delete of previous attempt to process the " + call_type + " file:" + file_name + " table: " + table_name)

                    except Exception as e:

                        # Increment the failed attempts
                        records_deleted_failed_attempts += 1

                        # If there is an error and using databse postgresql
                        # Then rollback the commit??
                        if self.database_type == "postgresql":
                            self._conn.rollback()

                        # Print and log general fail comment
                        print "Database delete attempt " + str(records_deleted_failed_attempts) + " failed... " + file_name + " from table: " + table_name
                        logger.error("Database delete attempt " + str(records_deleted_failed_attempts) + " failed..." + file_name + " from table: " + table_name)
                        # Print traceback
                        traceback.print_exc()
                        # Print exception information to file
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # used to verify whether the applicationID is in the current table APPLICATION
    def verify(self,sql):
        if self._conn == None:
            self.connect()
            self._cursor.execute(sql)
            #self._conn.commit()
            return self._cursor.fetchone()
        else:
            self._cursor.execute(sql)
            #self._conn.commit()
            return self._cursor.fetchone() #None or not

    def executeParam(self, sql, param):
        #try:
        if self._conn == None:
            self.connect()
            self._cursor.execute(sql, param)
            #self._conn.commit()
            result = self._cursor.fetchall()  #fetchone(), fetchmany(n)
            return result  #return a tuple ((),())
        else:
            self._cursor.execute(sql, param)
            #self._conn.commit()
            result = self._cursor.fetchall()  #fetchone(), fetchmany(n)
            return result  #return a tuple ((),())
        #finally:
            #self.close()

    def connect(self):

        # Connect to MySQL
        if self.database_type == "mysql":

            if self._conn == None:
                self._conn = MySQLdb.connect(
                    host = self._host,
                    user = self._username,
                    passwd = self._password,
                    db = self._dbname,
                    port = self._port,
                    charset = self._charset
                )
                print "Connection to MySQL database established."

            if self._cursor == None:
                self._cursor = self._conn.cursor()
                self._cursor.connection.autocommit(True)

        # Connect to PostgreSQL
        if self.database_type == "postgresql":

            if self._conn == None:
                # get a connection, if a connect cannot be made an exception will be raised here
                self._conn = psycopg2.connect("host=" + self._host +  " dbname=" + self._dbname + " user=" + self._username + " password=" + self._password + " port=" + str(self._port))
                self._conn.autocommit = True

            if self._cursor == None:
                # conn.cursor will return a cursor object, you can use this cursor to perform queries
                self._cursor = self._conn.cursor()
                print "Connection to PostgreSQL database established."

    def close(self):
        if self._cursor != None:
            self._cursor.close()
            self._cursor = None
        if self._conn != None:
            self._conn.close()
            self._conn = None
        print 'Connection to database closed successfully.'


# Reset the database using os.system command line
def reset_database(database_args, args_array):

    print "Resetting database contents...."

    try:
        # Set the variables to pass to SQL
        user = database_args['user']
        passwd = database_args['passwd']
        host = database_args['host']
        port = database_args['port']
        db = database_args['db']
        filename = args_array['database_reset_file']

        if args_array['database_type'] == "mysql":
            command = """mysql -u %s -p="%s" -h %s -P %s %s < %s""" %(user, passwd, host, port, db, filename)
            print command
            command_return_string = os.system(command)
            print command_return_string
        else:
            print "PostgreSQL cannot be automatically reset from commmand argument...."

    except Exception as e:
        traceback.print_exc()

# This function accepts a table name and a dictionary with keys as column names and values as data.
# It builds an sql query out of this array.
def build_sql_insert_query(insert_data_array, args_array):

    uspto_xml_format = args_array['uspto_xml_format']

    # Set a length counter used to find when the last item is appended to query string
    array_length_counter = 1
    length_of_array = len(insert_data_array) - 1
    # Pass the table name to variable
    table_name = insert_data_array['table_name']
    # Pop the table name off the array to be stored into database
    del insert_data_array['table_name']

    sql_query_string = "INSERT INTO " + table_name + " "
    sql_column_string = "("
    sql_value_string = " VALUES ("
    # Concatenate the list of keys and values to sql format
    for key, value in insert_data_array.items():

        # Don't escape values that are None (NULL)
        if value is not None and isinstance(value, int) == False:
            # escape all values for sql insertion
            value = USPTOSanitizer.escape_value_for_sql(str(value.encode('utf-8')))
            # Since postgresql uses `$` as delimiter, must  strip from first and last char
            value = value.strip("$").replace("$$$", "$").replace("$$", "$")

        # If the last item in the array then append line without comma at end
        if length_of_array == array_length_counter:
            sql_column_string += key
            # Check for None value and append
            if value == None:
                sql_value_string += 'NULL'
            else:
                # PostgreSQL strings will be escaped slightly different than MySQL
                if args_array['database_type'] == 'postgresql':
                    sql_value_string += "$$" + str(value)+ "$$"
                elif args_array['database_type'] == 'mysql':
                    sql_value_string += '"' + str(value) + '"'
        # If not the last item then append with comma
        else:
            sql_column_string += key + ", "
            # Check if value is None
            if value == None:
                sql_value_string +=  'NULL,'
            else:
                if args_array['database_type'] == 'postgresql':
                    sql_value_string +=  "$$" + str(value) + "$$,"
                elif args_array['database_type'] == 'mysql':
                    sql_value_string += '"' + str(value) + '",'
        array_length_counter += 1
    # Add the closing bracket
    sql_column_string += ") "
    sql_value_string += ");"

    # Concatnate the pieces of the query
    sql_query_string += sql_column_string + sql_value_string
    # Return the query string
    return sql_query_string
