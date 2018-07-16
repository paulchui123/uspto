# Used to operate the MySQL database.
# Remind to replace the arguments in the following funciton '__init__' with your own database parameters

import MySQLdb
import psycopg2
import traceback
import sys
import os

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


    def load(self,sql, args_array, logger):

        #print sql

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Execute the query pass into funtion
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
            print "Database (" + self.database_type + ") query failed... " + args_array['file_name'] + " into table: " + args_array['table_name'] + " Document ID Number " + args_array['document_id']
            logger.error("Database (" + self.database_type + ") query failed..." + args_array['file_name'] + " into table: " + args_array['table_name'] + " Document ID Number " + args_array['document_id'])
            # Print traceback
            traceback.print_exc()
            # Print exception information to file
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # used to retrieve ID buy matching fields of values
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
                print "Connection to database established."

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
                print "Connection to database established."

    def close(self):
        if self._cursor != None:
            self._cursor.close()
            self._cursor = None
        if self._conn != None:
            self._conn.close()
            self._conn = None
        print 'Connection to database closed successfully.'
