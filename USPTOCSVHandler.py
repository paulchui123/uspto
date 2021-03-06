# USPTOCSVHandler.py
# USPTO Bulk Data Parser - Processes for Managing CSV files
# Description: Processes prepares and deletes CSV files.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/uspto

# Import Python Modules
import csv
import time
import traceback
import os
import sys

# Import USPTO Parser Functions
import USPTOLogger

# Function used to open the required csv files and create a csv.DictWrite object
# for each one.  This function also creates arrays of table column names for each table
# and returns both the csv.DictWrite and table column arrays back to the args_array.
def open_csv_files(file_type, file_name, csv_directory):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Create an array of files to append to
    field_names_array = {}
    csv_writer_array = {}

    # Define filename for csv file
    csv_file_name = file_name + '.csv'

    # If the grant CSV file will be written
    if file_type == "grant":

        # Create array of field names for each application table and append to array to be passed back with args array
        field_names_array['grant'] = ['GrantID', 'IssueDate', 'Kind', 'USSeriesCode', 'Title', 'Abstract', 'Claims', 'ClaimsNum', 'DrawingsNum', 'FiguresNum', 'GrantLength', 'ApplicationID', 'FileDate', 'AppType', 'FileName']
        field_names_array['applicant'] = ['GrantID', 'Position', 'OrgName', 'FirstName', 'LastName', 'City', 'State', 'Country', 'FileName']
        field_names_array['examiner'] = ['GrantID', 'Position', 'LastName', 'FirstName', 'Department', 'FileName']
        field_names_array['agent'] = ['GrantID', 'Position', 'OrgName', 'LastName', 'FirstName', 'Country', 'FileName']
        field_names_array['assignee'] = ['GrantID', 'Position', 'OrgName', 'Role', 'City', 'State', 'Country', 'FileName']
        field_names_array['inventor'] = ['GrantID', 'Position', 'FirstName', 'LastName', 'City', 'State', 'Country', 'Nationality', 'Residence', 'FileName']
        field_names_array['gracit'] = ['GrantID', 'Position', 'CitedID', 'Kind', 'Name', 'Date', 'Country', 'Category', 'FileName']
        field_names_array['forpatcit'] = ['GrantID', 'Position', 'CitedID', 'Kind', 'Name', 'Date', 'Country', 'Category', 'FileName']
        field_names_array['nonpatcit'] = ['GrantID', 'Position', 'Citation', 'Category', 'FileName']
        field_names_array['usclass'] = ['GrantID','Position', 'Class', 'SubClass', 'Malformed', 'FileName']
        field_names_array['intclass'] = ['GrantID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['cpcclass'] = ['GrantID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']

        # Define all the dictionary arrays to hold writers and filenames
        csv_writer_array['grant'] = {}
        csv_writer_array['applicant'] = {}
        csv_writer_array['examiner'] = {}
        csv_writer_array['agent'] = {}
        csv_writer_array['assignee'] = {}
        csv_writer_array['inventor'] = {}
        csv_writer_array['gracit'] = {}
        csv_writer_array['forpatcit'] = {}
        csv_writer_array['nonpatcit'] = {}
        csv_writer_array['usclass'] = {}
        csv_writer_array['intclass'] = {}
        csv_writer_array['cpcclass'] = {}

        # Define all the .csv filenames fullpath and append to array
        csv_writer_array['grant']['csv_file_name'] = csv_directory + 'CSV_G/grant_' + csv_file_name
        csv_writer_array['applicant']['csv_file_name'] = csv_directory + 'CSV_G/applicant_' + csv_file_name
        csv_writer_array['examiner']['csv_file_name'] = csv_directory + 'CSV_G/examiner_' + csv_file_name
        csv_writer_array['agent']['csv_file_name'] = csv_directory + 'CSV_G/agent_' + csv_file_name
        csv_writer_array['assignee']['csv_file_name'] = csv_directory + 'CSV_G/assignee_' + csv_file_name
        csv_writer_array['inventor']['csv_file_name'] = csv_directory + 'CSV_G/inventor_' + csv_file_name
        csv_writer_array['gracit']['csv_file_name'] = csv_directory + 'CSV_G/gracit_' + csv_file_name
        csv_writer_array['forpatcit']['csv_file_name'] = csv_directory + 'CSV_G/forpatcit_' + csv_file_name
        csv_writer_array['nonpatcit']['csv_file_name'] = csv_directory + 'CSV_G/nonpatcit_' + csv_file_name
        csv_writer_array['usclass']['csv_file_name'] = csv_directory + 'CSV_G/usclass_' + csv_file_name
        csv_writer_array['intclass']['csv_file_name'] = csv_directory + 'CSV_G/intclass_' + csv_file_name
        csv_writer_array['cpcclass']['csv_file_name'] = csv_directory + 'CSV_G/cpcclass_' + csv_file_name

        # Define all the dictionary arrays to hold writers and filenames
        csv_writer_array['grant']['file'] = open(csv_writer_array['grant']['csv_file_name'], 'w')
        csv_writer_array['applicant']['file'] = open(csv_writer_array['applicant']['csv_file_name'], 'w')
        csv_writer_array['examiner']['file'] = open(csv_writer_array['examiner']['csv_file_name'], 'w')
        csv_writer_array['agent']['file'] = open(csv_writer_array['agent']['csv_file_name'], 'w')
        csv_writer_array['assignee']['file'] = open(csv_writer_array['assignee']['csv_file_name'], 'w')
        csv_writer_array['inventor']['file'] = open(csv_writer_array['inventor']['csv_file_name'], 'w')
        csv_writer_array['gracit']['file'] = open(csv_writer_array['gracit']['csv_file_name'], 'w')
        csv_writer_array['forpatcit']['file'] = open(csv_writer_array['forpatcit']['csv_file_name'], 'w')
        csv_writer_array['nonpatcit']['file'] = open(csv_writer_array['nonpatcit']['csv_file_name'], 'w')
        csv_writer_array['usclass']['file'] = open(csv_writer_array['usclass']['csv_file_name'], 'w')
        csv_writer_array['intclass']['file'] = open(csv_writer_array['intclass']['csv_file_name'], 'w')
        csv_writer_array['cpcclass']['file'] = open(csv_writer_array['cpcclass']['csv_file_name'], 'w')

        # Open all CSV files to write to and append to array
        csv_writer_array['grant']['csv_writer'] = csv.DictWriter(csv_writer_array['grant']['file'], fieldnames = field_names_array['grant'], delimiter = '|')
        csv_writer_array['applicant']['csv_writer'] = csv.DictWriter(csv_writer_array['applicant']['file'], fieldnames = field_names_array['applicant'], delimiter = '|')
        csv_writer_array['examiner']['csv_writer'] = csv.DictWriter(csv_writer_array['examiner']['file'], fieldnames = field_names_array['examiner'], delimiter = '|')
        csv_writer_array['agent']['csv_writer'] = csv.DictWriter(csv_writer_array['agent']['file'], fieldnames = field_names_array['agent'], delimiter = '|')
        csv_writer_array['assignee']['csv_writer'] = csv.DictWriter(csv_writer_array['assignee']['file'], fieldnames = field_names_array['assignee'], delimiter = '|')
        csv_writer_array['inventor']['csv_writer'] = csv.DictWriter(csv_writer_array['inventor']['file'], fieldnames = field_names_array['inventor'], delimiter = '|')
        csv_writer_array['gracit']['csv_writer'] = csv.DictWriter(csv_writer_array['gracit']['file'], fieldnames = field_names_array['gracit'], delimiter = '|')
        csv_writer_array['forpatcit']['csv_writer'] = csv.DictWriter(csv_writer_array['forpatcit']['file'], fieldnames = field_names_array['forpatcit'], delimiter = '|')
        csv_writer_array['nonpatcit']['csv_writer'] = csv.DictWriter(csv_writer_array['nonpatcit']['file'], fieldnames = field_names_array['nonpatcit'], delimiter = '|')
        csv_writer_array['usclass']['csv_writer'] = csv.DictWriter(csv_writer_array['usclass']['file'], fieldnames = field_names_array['usclass'], delimiter = '|')
        csv_writer_array['intclass']['csv_writer'] = csv.DictWriter(csv_writer_array['intclass']['file'], fieldnames = field_names_array['intclass'], delimiter = '|')
        csv_writer_array['cpcclass']['csv_writer'] = csv.DictWriter(csv_writer_array['cpcclass']['file'], fieldnames = field_names_array['cpcclass'], delimiter = '|')

        # Write the header to each file
        csv_writer_array['grant']['csv_writer'].writeheader()
        csv_writer_array['applicant']['csv_writer'].writeheader()
        csv_writer_array['examiner']['csv_writer'].writeheader()
        csv_writer_array['agent']['csv_writer'].writeheader()
        csv_writer_array['assignee']['csv_writer'].writeheader()
        csv_writer_array['inventor']['csv_writer'].writeheader()
        csv_writer_array['gracit']['csv_writer'].writeheader()
        csv_writer_array['forpatcit']['csv_writer'].writeheader()
        csv_writer_array['nonpatcit']['csv_writer'].writeheader()
        csv_writer_array['usclass']['csv_writer'].writeheader()
        csv_writer_array['intclass']['csv_writer'].writeheader()
        csv_writer_array['cpcclass']['csv_writer'].writeheader()

    # If the application CSV file will be written
    elif file_type == "application":

        # Create array of field names for each application table and append to array to be passed back with args array
        field_names_array['application'] = ['ApplicationID', 'PublicationID', 'FileDate', 'Kind', 'USSeriesCode', 'AppType', 'PublishDate', 'Title', 'Abstract', 'ClaimsNum', 'DrawingsNum', 'FiguresNum', 'FileName']
        field_names_array['agent'] = ['ApplicationID', 'Position', 'OrgName', 'LastName', 'FirstName', 'Country', 'FileName']
        field_names_array['assignee'] = ['ApplicationID', 'Position', 'OrgName', 'Role', 'City', 'State', 'Country', 'FileName']
        field_names_array['inventor'] = ['ApplicationID', 'Position', 'FirstName', 'LastName', 'City', 'State', 'Country', 'Nationality', 'Residence', 'FileName']
        field_names_array['applicant'] = ['ApplicationID', 'Position', 'OrgName', 'FirstName', 'LastName', 'City', 'State', 'Country', 'FileName']
        field_names_array['usclass'] = ['ApplicationID','Position', 'Class', 'SubClass', 'Malformed', 'FileName']
        field_names_array['intclass'] = ['ApplicationID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['cpcclass'] = ['ApplicationID', 'Position', 'Section', 'Class', 'SubClass', 'MainGroup', 'SubGroup', 'Malformed', 'FileName']
        field_names_array['foreignpriority'] = ['ApplicationID', 'DocumentID', 'Position', 'Kind', 'Country', 'PriorityDate', 'FileName']

        # Define all the dicionaries to hold the csv data
        csv_writer_array['application'] = {}
        csv_writer_array['agent'] = {}
        csv_writer_array['assignee'] = {}
        csv_writer_array['inventor'] = {}
        csv_writer_array['applicant'] = {}
        csv_writer_array['usclass'] = {}
        csv_writer_array['intclass'] = {}
        csv_writer_array['cpcclass'] = {}
        csv_writer_array['foreignpriority'] = {}

        # Define all the .csv filenames fullpath and append to array
        csv_writer_array['application']['csv_file_name'] = csv_directory + 'CSV_A/application_' + csv_file_name
        csv_writer_array['agent']['csv_file_name'] = csv_directory + 'CSV_A/agent_' + csv_file_name
        csv_writer_array['assignee']['csv_file_name'] = csv_directory + 'CSV_A/assignee_' + csv_file_name
        csv_writer_array['inventor']['csv_file_name'] = csv_directory + 'CSV_A/inventor_' + csv_file_name
        csv_writer_array['applicant']['csv_file_name'] = csv_directory + 'CSV_A/applicant_' + csv_file_name
        csv_writer_array['usclass']['csv_file_name'] = csv_directory + 'CSV_A/usclass_' + csv_file_name
        csv_writer_array['intclass']['csv_file_name'] = csv_directory + 'CSV_A/intclass_' + csv_file_name
        csv_writer_array['cpcclass']['csv_file_name'] = csv_directory + 'CSV_A/cpcclass_' + csv_file_name
        csv_writer_array['foreignpriority']['csv_file_name'] = csv_directory + 'CSV_A/foreignpriority_' + csv_file_name

        # Define all the dicionaries to hold the csv data
        csv_writer_array['application']['file'] = open(csv_writer_array['application']['csv_file_name'], 'w')
        csv_writer_array['agent']['file'] = open(csv_writer_array['agent']['csv_file_name'], 'w')
        csv_writer_array['assignee']['file'] = open(csv_writer_array['assignee']['csv_file_name'], 'w')
        csv_writer_array['inventor']['file'] = open(csv_writer_array['inventor']['csv_file_name'], 'w')
        csv_writer_array['applicant']['file'] = open(csv_writer_array['applicant']['csv_file_name'], 'w')
        csv_writer_array['usclass']['file'] = open(csv_writer_array['usclass']['csv_file_name'], 'w')
        csv_writer_array['intclass']['file'] = open(csv_writer_array['intclass']['csv_file_name'], 'w')
        csv_writer_array['cpcclass']['file'] = open(csv_writer_array['cpcclass']['csv_file_name'], 'w')
        csv_writer_array['foreignpriority']['file'] = open(csv_writer_array['foreignpriority']['csv_file_name'], 'w')

        # Open all CSV files to write to and append to array
        csv_writer_array['application']['csv_writer'] = csv.DictWriter(csv_writer_array['application']['file'], fieldnames = field_names_array['application'], delimiter = '|')
        csv_writer_array['agent']['csv_writer'] = csv.DictWriter(csv_writer_array['agent']['file'], fieldnames = field_names_array['agent'], delimiter = '|')
        csv_writer_array['assignee']['csv_writer'] = csv.DictWriter(csv_writer_array['assignee']['file'], fieldnames = field_names_array['assignee'], delimiter = '|')
        csv_writer_array['inventor']['csv_writer'] = csv.DictWriter(csv_writer_array['inventor']['file'], fieldnames = field_names_array['inventor'], delimiter = '|')
        csv_writer_array['applicant']['csv_writer'] = csv.DictWriter(csv_writer_array['applicant']['file'], fieldnames = field_names_array['applicant'], delimiter = '|')
        csv_writer_array['usclass']['csv_writer'] = csv.DictWriter(csv_writer_array['usclass']['file'], fieldnames = field_names_array['usclass'], delimiter = '|')
        csv_writer_array['intclass']['csv_writer'] = csv.DictWriter(csv_writer_array['intclass']['file'], fieldnames = field_names_array['intclass'], delimiter = '|')
        csv_writer_array['cpcclass']['csv_writer'] = csv.DictWriter(csv_writer_array['cpcclass']['file'], fieldnames = field_names_array['cpcclass'], delimiter = '|')
        csv_writer_array['foreignpriority']['csv_writer'] = csv.DictWriter(csv_writer_array['foreignpriority']['file'], fieldnames = field_names_array['foreignpriority'], delimiter = '|')

        # Write header for all application csv files
        csv_writer_array['application']['csv_writer'].writeheader()
        csv_writer_array['agent']['csv_writer'].writeheader()
        csv_writer_array['assignee']['csv_writer'].writeheader()
        csv_writer_array['inventor']['csv_writer'].writeheader()
        csv_writer_array['applicant']['csv_writer'].writeheader()
        csv_writer_array['usclass']['csv_writer'].writeheader()
        csv_writer_array['intclass']['csv_writer'].writeheader()
        csv_writer_array['cpcclass']['csv_writer'].writeheader()
        csv_writer_array['foreignpriority']['csv_writer'].writeheader()

    # Write message to stdout and log that all csv files are open
    print '[Opened all .csv files for ' + file_type + ' ' + file_name + ' storage Time: {0}]'.format(time.strftime('%c'))
    logger.info('[Opened all .csv files for ' + file_type + ' ' + file_name + ' storage Time: {0}]'.format(time.strftime('%c')))

    # Return the array
    return csv_writer_array
    #return True

# Function used to close all .csv files in array
def close_csv_files(args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout and log
    print '[Cleaning up .csv files... ]'
    logger.info('[Cleaning up .csv files... ]')

    # Loop through each file in array of open csv files
    for key, csv_file in args_array['csv_file_array'].items():
        try:
            # Close file being written to
            csv_file['file'].close()
            # Print message to stdout and log
            print 'Closed .csv file: ' + csv_file['csv_file_name'] + ' Time: {0}'.format(time.strftime('%c'))
            logger.info('Closed .csv file: ' + csv_file['csv_file_name'] + ' Time: {0}'.format(time.strftime('%c')))
            # Remove csv file from the CSV directory if 'csv' not in args_array['command_args']
        except Exception as e:
            # Print exception information to file
            print 'Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.error('Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
            traceback.print_exc()

# Function used to close all csv files in array
def delete_csv_files(args_array):

    # Import logger
    logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")

    # Print message to stdout and log
    print '[Cleaning up .csv files... ]'
    logger.info('[Cleaning up .csv files... ]')

    # Loop through each file in array of open csv files
    for key, csv_file in args_array['csv_file_array'].items():
        try:
            # Remove csv file from the CSV directory if 'csv' not in args_array['command_args']
            if os.path.exists(csv_file['csv_file_name']):
                os.remove(csv_file['csv_file_name'])
            # Print message to stdout and log
            print 'Removed .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.info('Removed .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
        except Exception as e:
            # Print exception information to file
            print 'Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c'))
            logger.error('Error removing .csv file: {0} Time: {1}'.format(csv_file['csv_file_name'], time.strftime('%c')))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
            traceback.print_exc()
